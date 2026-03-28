export const initColumnPickerForms = (root = document) => {
    root.querySelectorAll("[data-column-picker-form]").forEach((form) => {
        form.addEventListener("submit", () => {
            const output = form.querySelector("[data-columns-output]");
            if (!output) {
                return;
            }

            const selectedColumns = Array.from(
                form.querySelectorAll("[data-column-option]:checked"),
                (input) => input.value,
            );
            output.value = selectedColumns.join(",");
        });
    });
};
