export const initImportLedger = (root = document) => {
    if (!window.flatpickr) {
        return;
    }

    root.querySelectorAll("[data-import-date-field]").forEach((input) => {
        const fieldLabel = document.querySelector(`label[for="${input.id}"]`);
        window.flatpickr(input, {
            altInput: true,
            altFormat: "m/d/Y",
            altInputClass: "tw-import-input tw-import-filter-input tw-import-date-alt",
            allowInput: true,
            dateFormat: "Y-m-d",
            disableMobile: true,
            position: "auto left",
            onReady(_selectedDates, _dateStr, instance) {
                instance.calendarContainer.classList.add("tw-import-datepicker");
                if (instance.altInput) {
                    instance.altInput.placeholder = "mm/dd/yyyy";
                    instance.altInput.setAttribute(
                        "aria-label",
                        input.getAttribute("aria-label")
                        || fieldLabel?.textContent?.trim()
                        || "Date",
                    );
                    instance.altInput.addEventListener("keydown", (event) => {
                        if (event.key === "Escape") {
                            instance.clear();
                        }
                    });
                    instance.altInput.addEventListener("blur", () => {
                        if (!instance.altInput.value.trim()) {
                            instance.clear();
                        }
                    });
                }
            },
        });
    });
};

initImportLedger();
