const parseDatasetBoolean = (value, fallback) => {
    if (value === undefined) {
        return fallback;
    }

    return value === "true";
};

export const initEnhancedSelects = (root = document) => {
    if (!window.Choices) {
        return;
    }

    root.querySelectorAll("[data-choice-select]").forEach((select) => {
        if (!(select instanceof HTMLSelectElement) || select.dataset.choiceInitialized === "true") {
            return;
        }

        const isMultiple = select.multiple;
        const placeholderOption = select.querySelector('option[value=""]');
        const placeholderValue = select.dataset.choicePlaceholder
            || placeholderOption?.textContent?.trim()
            || "";

        new window.Choices(select, {
            searchEnabled: parseDatasetBoolean(
                select.dataset.choiceSearchEnabled,
                isMultiple,
            ),
            removeItemButton: parseDatasetBoolean(
                select.dataset.choiceRemoveButton,
                isMultiple,
            ),
            shouldSort: parseDatasetBoolean(select.dataset.choiceShouldSort, false),
            itemSelectText: select.dataset.choiceSelectText || "",
            allowHTML: false,
            position: select.dataset.choicePosition || "bottom",
            placeholder: Boolean(placeholderValue),
            placeholderValue,
            classNames: {
                containerOuter: ["choices", "tw-choice-select"],
            },
        });

        select.dataset.choiceInitialized = "true";
    });
};
