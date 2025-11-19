package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/joho/godotenv"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// ----- Config -----

const (
	MaxPagesPerRun  = 500
	RequestTimeout  = 10 * time.Second
	PolitenessDelay = 500 * time.Millisecond
	MaxBodyBytes    = 2 * 1024 * 1024
	MaxDepth        = 5
	MaxTextChars    = 70000
)

// ---------------- UTF-8 SAFE ------------------

func safeUTF8(s string) string {
	return strings.ToValidUTF8(s, "")
}

// ----------------------------------------------

// Page stored in MongoDB
type Page struct {
	URL       string    `bson:"url"`
	Title     string    `bson:"title"`

	Snippet   string    `bson:"snippet"`    // NEW
	Favicon   string    `bson:"favicon"`    // NEW
	SiteName  string    `bson:"site_name"`  // NEW
	Image     string    `bson:"image"`      // NEW

	Text      string    `bson:"text"`
	Links     []string  `bson:"links"`
	CrawlTime time.Time `bson:"crawl_time"`
}

// ----- Env -----

func getEnv(key, def string) string {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	return v
}

// ----- Domain helpers -----

func isAllowedDomain(u *url.URL, allowedDomains []string) bool {
	if len(allowedDomains) == 0 {
		return true
	}
	host := u.Hostname()
	for _, d := range allowedDomains {
		if strings.HasSuffix(host, d) {
			return true
		}
	}
	return false
}

func normalizeURL(base *url.URL, href string) (*url.URL, error) {
	href = strings.TrimSpace(href)
	if href == "" {
		return nil, fmt.Errorf("empty href")
	}
	parsed, err := url.Parse(href)
	if err != nil {
		return nil, err
	}
	if !parsed.IsAbs() {
		parsed = base.ResolveReference(parsed)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, fmt.Errorf("unsupported scheme")
	}
	parsed.Fragment = ""
	return parsed, nil
}

// ----- Mongo Setup -----

func connectMongo(ctx context.Context) (*mongo.Client, *mongo.Collection, error) {
	uri := getEnv("MONGO_URI", "")
	dbName := getEnv("MONGO_DB_NAME", "basic_search_engine")

	if uri == "" {
		return nil, nil, fmt.Errorf("MONGO_URI not set")
	}

	clientOpts := options.Client().ApplyURI(uri)
	client, err := mongo.Connect(ctx, clientOpts)
	if err != nil {
		return nil, nil, err
	}
	if err := client.Ping(ctx, nil); err != nil {
		return nil, nil, err
	}

	col := client.Database(dbName).Collection("pages")
	return client, col, nil
}

func upsertPage(ctx context.Context, col *mongo.Collection, p Page) error {
	// SANITIZE EVERYTHING → UTF-8 SAFE
	p.URL = safeUTF8(p.URL)
	p.Title = safeUTF8(p.Title)
	p.Snippet = safeUTF8(p.Snippet)
	p.Favicon = safeUTF8(p.Favicon)
	p.SiteName = safeUTF8(p.SiteName)
	p.Image = safeUTF8(p.Image)
	p.Text = safeUTF8(p.Text)

	filter := bson.M{"url": p.URL}
	update := bson.M{"$set": p}
	opts := options.Update().SetUpsert(true)

	_, err := col.UpdateOne(ctx, filter, update, opts)
	return err
}

func pageExists(ctx context.Context, col *mongo.Collection, pageURL string) (bool, error) {
	err := col.FindOne(ctx, bson.M{"url": pageURL}).Err()
	if err == mongo.ErrNoDocuments {
		return false, nil
	}
	return err == nil, err
}

// ----- Fetch -----

func fetchPage(u string) (*goquery.Document, error) {
	client := &http.Client{
		Timeout: RequestTimeout,
	}

	resp, err := client.Get(u)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	contentType := resp.Header.Get("Content-Type")
	if !strings.Contains(contentType, "text/html") {
		return nil, fmt.Errorf("non-html content type: %s", contentType)
	}

	limited := io.LimitReader(resp.Body, MaxBodyBytes)
	body, err := io.ReadAll(limited)
	if err != nil {
		return nil, err
	}

	return goquery.NewDocumentFromReader(bytes.NewReader(body))
}

// ----- Extract Page (Upgraded) -----

func extractPage(u string, doc *goquery.Document) Page {

	parsedURL, _ := url.Parse(u)

	// TITLE
	title := strings.TrimSpace(doc.Find("title").First().Text())

	// SNIPPET PRIORITY:
	snippet := ""

	// 1. meta description
	if desc, ok := doc.Find(`meta[name="description"]`).Attr("content"); ok {
		snippet = strings.TrimSpace(desc)
	}

	// 2. og:description
	if snippet == "" {
		if og, ok := doc.Find(`meta[property="og:description"]`).Attr("content"); ok {
			snippet = strings.TrimSpace(og)
		}
	}

	// 3. first <p>
	if snippet == "" {
		doc.Find("p").Each(func(i int, s *goquery.Selection) {
			txt := strings.TrimSpace(s.Text())
			if len(txt) > 40 && snippet == "" {
				snippet = txt
			}
		})
	}

	// 4. fallback
	if snippet == "" {
		raw := strings.TrimSpace(doc.Find("body").Text())
		runes := []rune(raw)
		if len(runes) > 300 {
			snippet = string(runes[:300])
		} else {
			snippet = raw
		}
	}

	// FAVICON
	favicon := ""
	doc.Find("link").Each(func(i int, s *goquery.Selection) {
		rel, _ := s.Attr("rel")
		href, _ := s.Attr("href")
		if strings.Contains(strings.ToLower(rel), "icon") {
			favicon = href
		}
	})

	if favicon != "" {
		fu, err := url.Parse(favicon)
		if err == nil && !fu.IsAbs() {
			fu = parsedURL.ResolveReference(fu)
		}
		favicon = fu.String()
	}

	// SITE NAME
	siteName := parsedURL.Hostname()
	if sn, ok := doc.Find(`meta[property="og:site_name"]`).Attr("content"); ok {
		if strings.TrimSpace(sn) != "" {
			siteName = sn
		}
	}

	// IMAGE
	img := ""
	if ogImg, ok := doc.Find(`meta[property="og:image"]`).Attr("content"); ok {
		img = ogImg
	}
	if img == "" {
		if twImg, ok := doc.Find(`meta[name="twitter:image"]`).Attr("content"); ok {
			img = twImg
		}
	}
	if img == "" {
		doc.Find("img").Each(func(i int, s *goquery.Selection) {
			if img != "" {
				return
			}
			src, ok := s.Attr("src")
			if !ok || src == "" {
				return
			}
			if strings.Contains(src, "logo") {
				return
			}
			img = src
		})
	}
	if img != "" {
		iu, err := url.Parse(img)
		if err == nil && !iu.IsAbs() {
			iu = parsedURL.ResolveReference(iu)
		}
		img = iu.String()
	}

	// FULL TEXT
	text := strings.TrimSpace(doc.Find("body").Text())
	runes := []rune(text)
	if len(runes) > MaxTextChars {
		text = string(runes[:MaxTextChars])
	}

	// LINKS
	var links []string
	doc.Find("a[href]").Each(func(i int, s *goquery.Selection) {
		h, _ := s.Attr("href")
		links = append(links, safeUTF8(h))
	})

	return Page{
		URL:       u,
		Title:     title,
		Snippet:   snippet,
		Favicon:   favicon,
		SiteName:  siteName,
		Image:     img,
		Text:      text,
		Links:     links,
		CrawlTime: time.Now().UTC(),
	}
}

// ----- Crawling -----

func crawlSeeds(ctx context.Context, col *mongo.Collection) error {

	seedsEnv := getEnv("SEED_URLS", "")
	if seedsEnv == "" {
		return fmt.Errorf("SEED_URLS not set")
	}
	seeds := strings.Split(seedsEnv, ",")

	allowedDomainsEnv := getEnv("ALLOWED_DOMAINS", "")
	var allowedDomains []string
	if allowedDomainsEnv != "" {
		for _, d := range strings.Split(allowedDomainsEnv, ",") {
			allowedDomains = append(allowedDomains, strings.TrimSpace(d))
		}
	}

	type QueueItem struct {
		URL   string
		Depth int
	}

	queue := []QueueItem{}
	visited := make(map[string]bool)

	for _, s := range seeds {
		s = strings.TrimSpace(s)
		if s != "" {
			queue = append(queue, QueueItem{URL: s, Depth: 0})
		}
	}

	pagesCrawled := 0

	for len(queue) > 0 && pagesCrawled < MaxPagesPerRun {

		item := queue[0]
		queue = queue[1:]

		if visited[item.URL] {
			continue
		}
		visited[item.URL] = true

		parsedURL, err := url.Parse(item.URL)
		if err != nil {
			continue
		}

		if !isAllowedDomain(parsedURL, allowedDomains) {
			continue
		}

		exists, err := pageExists(ctx, col, item.URL)
		if err == nil && exists {
			continue
		}

		log.Printf("Fetching: %s", item.URL)
		doc, err := fetchPage(item.URL)
		if err != nil {
			log.Printf("error: %v", err)
			continue
		}

		page := extractPage(item.URL, doc)
		upsertPage(ctx, col, page)

		pagesCrawled++
		log.Printf("Crawled %d pages", pagesCrawled)

		if item.Depth < MaxDepth {
			for _, href := range page.Links {
				norm, err := normalizeURL(parsedURL, href)
				if err == nil && !visited[norm.String()] {
					queue = append(queue, QueueItem{URL: norm.String(), Depth: item.Depth + 1})
				}
			}
		}

		time.Sleep(PolitenessDelay)
	}

	return nil
}

func main() {
	godotenv.Load()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	client, col, err := connectMongo(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Disconnect(ctx)

	if err := crawlSeeds(ctx, col); err != nil {
		log.Fatal(err)
	}
}
