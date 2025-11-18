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
	MaxPagesPerRun  = 500                     // safety cap per run | original = 100
	RequestTimeout  = 10 * time.Second        // HTTP timeout
	PolitenessDelay = 500 * time.Millisecond  // delay between requests 
	MaxBodyBytes    = 2 * 1024 * 1024         // 1MB limit per page | original = 1 * 1024 * 1024
	MaxDepth        = 5                       // how many link "hops" from seeds | original = 2
	MaxTextChars    = 70000                   // truncate text to avoid huge docs | original = 50000
)

// Page represents a crawled page stored in MongoDB
type Page struct {
	URL       string    `bson:"url"`
	Title     string    `bson:"title"`
	Text      string    `bson:"text"`
	Links     []string  `bson:"links"`
	CrawlTime time.Time `bson:"crawl_time"`
}

// ----- Env helpers -----

func getEnv(key, def string) string {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	return v
}

// ----- Domain & URL helpers -----

func isAllowedDomain(u *url.URL, allowedDomains []string) bool {
	if len(allowedDomains) == 0 {
		return true // no restriction
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

	// Handle relative URLs
	if !parsed.IsAbs() {
		parsed = base.ResolveReference(parsed)
	}

	// Only http/https
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, fmt.Errorf("unsupported scheme: %s", parsed.Scheme)
	}

	// Remove fragment (#section)
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

	collection := client.Database(dbName).Collection("pages")
	return client, collection, nil
}

// Upsert page by URL (one document per URL)
func upsertPage(ctx context.Context, col *mongo.Collection, p Page) error {
	filter := bson.M{"url": p.URL}
	update := bson.M{"$set": p}
	opts := options.Update().SetUpsert(true)
	_, err := col.UpdateOne(ctx, filter, update, opts)
	return err
}

// Check if page already exists
func pageExists(ctx context.Context, col *mongo.Collection, pageURL string) (bool, error) {
	filter := bson.M{"url": pageURL}
	err := col.FindOne(ctx, filter).Err()
	if err == mongo.ErrNoDocuments {
		return false, nil
	}
	return err == nil, err
}

// ----- Fetch & extract -----

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

	// Limit body size
	limited := io.LimitReader(resp.Body, MaxBodyBytes)
	body, err := io.ReadAll(limited)
	if err != nil {
		return nil, err
	}

	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(body))
	if err != nil {
		return nil, err
	}

	return doc, nil
}

func extractPage(u string, doc *goquery.Document) Page {
	title := strings.TrimSpace(doc.Find("title").First().Text())

	// Extract visible text (very basic; can improve later)
	text := strings.TrimSpace(doc.Find("body").Text())

	// Truncate to avoid giant docs, but do it safely by runes (characters)
	runes := []rune(text)
	if len(runes) > MaxTextChars {
    	text = string(runes[:MaxTextChars])
	}

	// Extract raw links (will normalize later)
	var links []string
	doc.Find("a[href]").Each(func(i int, s *goquery.Selection) {
		href, _ := s.Attr("href")
		links = append(links, href)
	})

	return Page{
		URL:       u,
		Title:     title,
		Text:      text,
		Links:     links,
		CrawlTime: time.Now().UTC(),
	}
}

// ----- Crawling logic -----

func crawlSeeds(ctx context.Context, col *mongo.Collection) error {
	seedsEnv := getEnv("SEED_URLS", "")
	if seedsEnv == "" {
		return fmt.Errorf("SEED_URLS not set (e.g. https://example.com,https://blog.example.com)")
	}
	seeds := strings.Split(seedsEnv, ",")

	allowedDomainsEnv := getEnv("ALLOWED_DOMAINS", "") // e.g. "example.com,example.org"
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
			log.Printf("invalid url %s: %v", item.URL, err)
			continue
		}

		if !isAllowedDomain(parsedURL, allowedDomains) {
			log.Printf("skip url (domain not allowed): %s", item.URL)
			continue
		}

		// Optional: skip if already in DB (you can change this later to re-crawl old pages)
		exists, err := pageExists(ctx, col, item.URL)
		if err != nil {
			log.Printf("error checking existence for %s: %v", item.URL, err)
		}
		if exists {
			log.Printf("already in DB, skipping: %s", item.URL)
			continue
		}

		log.Printf("Fetching: %s (depth %d)", item.URL, item.Depth)

		doc, err := fetchPage(item.URL)
		if err != nil {
			log.Printf("error fetching %s: %v", item.URL, err)
			continue
		}

		page := extractPage(item.URL, doc)

		if err := upsertPage(ctx, col, page); err != nil {
			log.Printf("error upserting %s: %v", item.URL, err)
			continue
		}

		pagesCrawled++
		log.Printf("Crawled %d pages so far", pagesCrawled)

		// Enqueue new links (if depth allows)
		if item.Depth < MaxDepth {
			for _, href := range page.Links {
				norm, err := normalizeURL(parsedURL, href)
				if err != nil {
					continue
				}
				normStr := norm.String()
				if !visited[normStr] {
					queue = append(queue, QueueItem{URL: normStr, Depth: item.Depth+1})
				}
			}
		}

		time.Sleep(PolitenessDelay)
	}

	log.Printf("Finished crawl run. Pages crawled: %d", pagesCrawled)
	return nil
}

func main() {
	// Load .env file if present
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, relying on system environment variables")
	}

	// Overall timeout for one run
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	client, col, err := connectMongo(ctx)
	if err != nil {
		log.Fatalf("Failed to connect to MongoDB: %v", err)
	}
	defer func() {
		if err := client.Disconnect(ctx); err != nil {
			log.Printf("Error disconnecting Mongo: %v", err)
		}
	}()

	if err := crawlSeeds(ctx, col); err != nil {
		log.Fatalf("Crawler error: %v", err)
	}
}
