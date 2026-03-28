export const initDynamicFormsets = (root = document) => {
    root.querySelectorAll("[data-dynamic-formset]").forEach((formsetRoot) => {
        const rowsContainer = formsetRoot.querySelector("[data-formset-rows]");
        const emptyState = formsetRoot.querySelector("[data-formset-empty-state]");
        const template = formsetRoot.querySelector("[data-formset-template]");
        const section = formsetRoot.closest("[data-formset-section]");
        const addButton = section ? section.querySelector("[data-formset-add]") : null;
        const totalFormsInput = formsetRoot.querySelector('input[name$="-TOTAL_FORMS"]');

        if (!rowsContainer || !emptyState || !template || !addButton || !totalFormsInput) {
            return;
        }

        const syncEmptyState = () => {
            emptyState.classList.toggle("hidden", rowsContainer.children.length > 0);
        };

        const updateRowHeading = (row, label) => {
            const heading = row.querySelector("[data-formset-row-title]");
            if (heading) {
                heading.textContent = label;
            }
        };

        const wireRow = (row) => {
            const deleteInput = row.querySelector('input[type="checkbox"][name$="-DELETE"]');
            const heading = row.querySelector("[data-formset-row-title]");
            const defaultHeading = heading ? heading.textContent : "Row";
            if (!deleteInput) {
                return;
            }

            const syncDeleteState = () => {
                row.classList.toggle("opacity-60", deleteInput.checked);
                row.classList.toggle("ring-1", deleteInput.checked);
                row.classList.toggle("ring-brand-danger/30", deleteInput.checked);
                updateRowHeading(
                    row,
                    deleteInput.checked ? `Remove ${defaultHeading.toLowerCase()}` : defaultHeading,
                );
            };

            deleteInput.addEventListener("change", syncDeleteState);
            syncDeleteState();
        };

        const addRow = () => {
            const nextIndex = Number(totalFormsInput.value);
            const html = template.innerHTML.replaceAll("__prefix__", String(nextIndex));
            const fragment = document.createRange().createContextualFragment(html);
            const row = fragment.querySelector("[data-formset-row]");
            if (!row) {
                return;
            }

            row.querySelectorAll("input, select, textarea").forEach((field) => {
                if (field.type === "checkbox" || field.type === "radio") {
                    field.checked = false;
                } else if (field.type !== "hidden") {
                    field.value = "";
                }
            });
            wireRow(row);
            rowsContainer.appendChild(row);
            totalFormsInput.value = String(nextIndex + 1);
            syncEmptyState();
        };

        rowsContainer.querySelectorAll("[data-formset-row]").forEach(wireRow);
        addButton.addEventListener("click", addRow);
        syncEmptyState();
    });
};
