// ===== CONFIG =====
// Change this to your deployed backend URL when you host the API.
const API_BASE_URL = "http://localhost:8000";

const urlParams = new URLSearchParams(window.location.search);
const query = urlParams.get("q");

const homeView = document.getElementById("home");
const resultsView = document.getElementById("resultsPage");
const resultsLogo = document.getElementById("resultsLogo");
const resultsQueryInput = document.getElementById("resultsQueryInput");
const resultsInfo = document.getElementById("resultsInfo");
const resultsList = document.getElementById("resultsList");
const resultsLoading = document.getElementById("resultsLoading");
const resultsError = document.getElementById("resultsError");

// Clicking the logo on results page should go "home"
resultsLogo.addEventListener("click", () => {
  window.location.href = "/";
});

if (query && query.trim() !== "") {
  // Show results view & hide home view
  homeView.style.display = "none";
  resultsView.style.display = "flex";

  // Fill search box with current query
  resultsQueryInput.value = query;

  // Fetch results
  fetchResults(query);
} else {
  // Show home view only
  homeView.style.display = "flex";
  resultsView.style.display = "none";
}

function cleanSnippet(raw) {
  if (!raw) return "";

  let s = String(raw);

  // Remove any HTML tags like <div>...</div>
  s = s.replace(/<[^>]+>/g, " ");

  // Remove common CSS-ish noise characters if they appear a lot
  // (keeps punctuation like . , ? ! but drops things like { } < > = ; *)
  s = s.replace(/[{}<>;=*]+/g, " ");

  // Collapse whitespace
  s = s.replace(/\s+/g, " ").trim();

  // Keep it short
  if (s.length > 220) {
    s = s.slice(0, 220) + "…";
  }

  return s;
}

async function fetchResults(q) {
  resultsLoading.style.display = "flex";
  resultsError.style.display = "none";
  resultsList.innerHTML = "";
  resultsInfo.textContent = "";

  const started = performance.now();

  try {
    const response = await fetch(
      `${API_BASE_URL}/search?q=` + encodeURIComponent(q)
    );

    if (!response.ok) {
      throw new Error("Search request failed with status " + response.status);
    }

    const data = await response.json();
    const elapsedMs = performance.now() - started;

    const count = data.count ?? (data.results ? data.results.length : 0);
    const timeSeconds = (elapsedMs / 1000).toFixed(2);

    resultsInfo.textContent = `About ${count} result(s) in ${timeSeconds} seconds`;

    if (!data.results || data.results.length === 0) {
      resultsList.innerHTML = "<p>No results found.</p>";
      return;
    }

    data.results.forEach((item) => {
      const container = document.createElement("div");
      container.className = "result-item";

      const urlEl = document.createElement("div");
      urlEl.className = "result-url";
      urlEl.textContent = item.url;

      const titleEl = document.createElement("a");
      titleEl.className = "result-title";
      titleEl.href = item.url;
      titleEl.target = "_blank";
      titleEl.rel = "noopener noreferrer";
      titleEl.textContent = item.title || item.url;

      const snippetEl = document.createElement("div");
      snippetEl.className = "result-snippet";
      snippetEl.textContent = cleanSnippet(item.snippet || "");

      const horizontalRule = document.createElement("hr");
      horizontalRule.className = "horizontal-rule";

      container.appendChild(urlEl);
      container.appendChild(titleEl);
      container.appendChild(snippetEl);
      container.appendChild(horizontalRule);

      resultsList.appendChild(container);
    });
  } catch (err) {
    console.error(err);
    resultsError.style.display = "block";
    resultsError.textContent =
      "Error fetching results. Please try again later.";
  } finally {
    resultsLoading.style.display = "none";
  }
}
