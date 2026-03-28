import { createMutedMessage, createNoticeMessage } from "../core/dom.js";

export const initLiveRelationshipPickers = (root = document) => {
    root.querySelectorAll("[data-live-relationship-picker]").forEach((picker) => {
        const searchInput = picker.querySelector("[data-relationship-search]");
        const hiddenInputs = picker.querySelector("[data-relationship-hidden-inputs]");
        const selectedContainer = picker.querySelector("[data-relationship-selected]");
        const resultsContainer = picker.querySelector("[data-relationship-results]");
        const searchUrl = picker.dataset.searchUrl;
        const fieldName = picker.dataset.fieldName;
        const emptyText = picker.dataset.emptyText || "Start typing to search.";
        const noResultsText = picker.dataset.noResultsText || "No results found.";
        const selectedEmptyText = picker.dataset.selectedEmptyText || "No items selected.";
        const selectedItems = new Map();
        let debounceTimer = null;
        let controller = null;
        let activeResults = [];

        if (!searchInput || !hiddenInputs || !selectedContainer || !resultsContainer || !searchUrl || !fieldName) {
            return;
        }

        selectedContainer.querySelectorAll("[data-selected-id]").forEach((item) => {
            selectedItems.set(String(item.dataset.selectedId), {
                id: String(item.dataset.selectedId),
                label: item.dataset.selectedLabel || item.textContent.trim(),
                meta: item.dataset.selectedMeta || "",
            });
        });

        const syncHiddenInputs = () => {
            hiddenInputs.innerHTML = "";
            selectedItems.forEach((item) => {
                const input = document.createElement("input");
                input.type = "hidden";
                input.name = fieldName;
                input.value = item.id;
                hiddenInputs.appendChild(input);
            });
        };

        const renderSelected = () => {
            selectedContainer.innerHTML = "";
            if (!selectedItems.size) {
                selectedContainer.appendChild(createMutedMessage(selectedEmptyText, "relationshipSelectedEmpty"));
                return;
            }

            const list = document.createElement("div");
            list.className = "flex flex-wrap gap-2";
            selectedItems.forEach((item) => {
                const button = document.createElement("button");
                button.type = "button";
                button.className = "inline-flex items-center gap-2 rounded-full border border-brand-surface-border bg-brand-surface-soft px-4 py-2 text-sm font-semibold text-brand-text-soft transition duration-200 hover:border-brand-accent/30 hover:text-brand-text-base";
                button.dataset.selectedId = item.id;
                button.dataset.selectedLabel = item.label;
                button.dataset.selectedMeta = item.meta || "";

                const label = document.createElement("span");
                label.textContent = item.label;
                button.appendChild(label);

                const action = document.createElement("strong");
                action.className = "text-[0.72rem] uppercase tracking-[0.14em] text-brand-accent";
                action.textContent = "Remove";
                button.appendChild(action);

                button.addEventListener("click", () => {
                    selectedItems.delete(item.id);
                    syncHiddenInputs();
                    renderSelected();
                    renderResults(activeResults);
                });

                list.appendChild(button);
            });
            selectedContainer.appendChild(list);
        };

        const renderResults = (results) => {
            activeResults = results;
            resultsContainer.innerHTML = "";

            if (searchInput.value.trim().length < 2) {
                resultsContainer.appendChild(createMutedMessage(emptyText, "relationshipResultsEmpty"));
                return;
            }

            if (!results.length) {
                resultsContainer.appendChild(createMutedMessage(noResultsText, "relationshipResultsEmpty"));
                return;
            }

            const list = document.createElement("div");
            list.className = "flex flex-col gap-2";

            results.forEach((item) => {
                const row = document.createElement("button");
                row.type = "button";
                row.className = "flex w-full flex-col gap-1 rounded-[1rem] border border-transparent bg-brand-surface-soft/70 px-4 py-3 text-left transition duration-150 hover:border-brand-teal/20 hover:bg-brand-teal/8";
                if (selectedItems.has(String(item.id))) {
                    row.classList.add("border-brand-accent/30", "bg-brand-accent/8");
                }

                const label = document.createElement("span");
                label.className = "text-sm font-semibold text-brand-text-base";
                label.textContent = item.label;
                row.appendChild(label);

                if (item.meta) {
                    const meta = document.createElement("span");
                    meta.className = "text-xs leading-5 text-brand-text-muted";
                    meta.textContent = item.meta;
                    row.appendChild(meta);
                }

                row.addEventListener("click", () => {
                    const id = String(item.id);
                    if (selectedItems.has(id)) {
                        selectedItems.delete(id);
                    } else {
                        selectedItems.set(id, {
                            id,
                            label: item.label,
                            meta: item.meta || "",
                        });
                    }
                    syncHiddenInputs();
                    renderSelected();
                    renderResults(activeResults);
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
                    resultsContainer.appendChild(createNoticeMessage("Unable to load results right now."));
                });
        };

        searchInput.addEventListener("input", () => {
            window.clearTimeout(debounceTimer);
            debounceTimer = window.setTimeout(fetchResults, 180);
        });
        searchInput.addEventListener("search", fetchResults);

        syncHiddenInputs();
        renderSelected();
        renderResults([]);
    });
};
