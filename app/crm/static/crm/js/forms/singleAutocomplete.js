import { createMutedMessage, createNoticeMessage } from "../core/dom.js";

export const initSingleAutocompletes = (root = document) => {
    root.querySelectorAll("[data-single-autocomplete]").forEach((picker) => {
        const searchInput = picker.querySelector("input");
        const resultsContainer = picker.querySelector("[data-autocomplete-results]");
        const searchUrl = picker.dataset.searchUrl;
        const emptyText = picker.dataset.emptyText || "Start typing to search.";
        const noResultsText = picker.dataset.noResultsText || "No results found.";
        let debounceTimer = null;
        let controller = null;

        if (!searchInput || !resultsContainer || !searchUrl) {
            return;
        }

        const renderResults = (results) => {
            resultsContainer.innerHTML = "";

            if (searchInput.value.trim().length < 2) {
                resultsContainer.appendChild(createMutedMessage(emptyText, "autocompleteResultsEmpty"));
                return;
            }

            if (!results.length) {
                resultsContainer.appendChild(createMutedMessage(noResultsText, "autocompleteResultsEmpty"));
                return;
            }

            const list = document.createElement("div");
            list.className = "flex flex-col gap-2";

            results.forEach((item) => {
                const row = document.createElement("button");
                row.type = "button";
                row.className = "flex w-full flex-col gap-1 rounded-[1rem] border border-transparent bg-brand-surface-soft/70 px-4 py-3 text-left transition duration-150 hover:border-brand-teal/20 hover:bg-brand-teal/8";

                const label = document.createElement("span");
                label.className = "text-sm font-semibold text-brand-text-base";
                label.textContent = item.label;
                row.appendChild(label);

                row.addEventListener("click", () => {
                    searchInput.value = item.value || item.label || "";
                    resultsContainer.innerHTML = "";
                    searchInput.focus();
                });

                list.appendChild(row);
            });

            resultsContainer.appendChild(list);
        };

        const fetchResults = () => {
            const query = searchInput.value.trim();
            if (controller) {
                controller.abort();
            }
            if (query.length < 2) {
                renderResults([]);
                return;
            }

            controller = new AbortController();
            fetch(`${searchUrl}?q=${encodeURIComponent(query)}`, {
                headers: {"X-Requested-With": "XMLHttpRequest"},
                signal: controller.signal,
            })
                .then((response) => {
                    if (!response.ok) {
                        throw new Error("Request failed");
                    }
                    return response.json();
                })
                .then((payload) => {
                    renderResults(Array.isArray(payload.results) ? payload.results : []);
                })
                .catch((error) => {
                    if (error.name === "AbortError") {
                        return;
                    }
                    resultsContainer.innerHTML = "";
                    resultsContainer.appendChild(createNoticeMessage("Unable to load suggestions right now."));
                });
        };

        searchInput.addEventListener("input", () => {
            window.clearTimeout(debounceTimer);
            debounceTimer = window.setTimeout(fetchResults, 180);
        });
        searchInput.addEventListener("search", fetchResults);
        renderResults([]);
    });
};
