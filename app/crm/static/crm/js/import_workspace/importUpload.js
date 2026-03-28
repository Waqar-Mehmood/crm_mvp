const formatBytes = (bytes) => {
    if (!Number.isFinite(bytes) || bytes <= 0) {
        return "0 B";
    }

    const units = ["B", "KB", "MB", "GB"];
    let value = bytes;
    let unitIndex = 0;
    while (value >= 1024 && unitIndex < units.length - 1) {
        value /= 1024;
        unitIndex += 1;
    }
    const precision = value >= 10 || unitIndex === 0 ? 0 : 1;
    return `${value.toFixed(precision)} ${units[unitIndex]}`;
};

export const initImportUpload = (root = document) => {
    root.querySelectorAll("[data-import-upload]").forEach((uploadForm) => {
        const fileInput = uploadForm.querySelector("[data-import-upload-input]");
        const addMoreButton = uploadForm.querySelector("[data-import-upload-trigger]");
        const selectedHeading = uploadForm.querySelector("[data-import-upload-heading]");
        const selectedList = uploadForm.querySelector("[data-import-upload-list]");
        const emptyState = uploadForm.querySelector("[data-import-upload-empty]");

        if (!fileInput || !addMoreButton || !selectedHeading || !selectedList || !emptyState) {
            return;
        }

        const selectedFiles = new Map();
        const fileKey = (file) => `${file.name}-${file.size}-${file.lastModified}`;

        const syncInputFiles = () => {
            const dataTransfer = new DataTransfer();
            selectedFiles.forEach((file) => dataTransfer.items.add(file));
            fileInput.files = dataTransfer.files;
        };

        const removeFile = (key) => {
            selectedFiles.delete(key);
            syncInputFiles();
            renderSelectedFiles();
        };

        const renderSelectedFiles = () => {
            const files = Array.from(selectedFiles.entries());
            selectedHeading.textContent = `Selected files (${files.length})`;
            selectedList.replaceChildren();
            emptyState.hidden = files.length > 0;

            files.forEach(([key, file]) => {
                const row = document.createElement("div");
                row.className = "flex items-center justify-between gap-3 rounded-lg border border-[#ddcfb6] bg-white/90 p-2.5 shadow-sm";

                const copy = document.createElement("div");
                copy.className = "min-w-0";

                const name = document.createElement("strong");
                name.className = "block truncate text-sm font-semibold text-panel-text";
                name.textContent = file.name;

                const meta = document.createElement("p");
                meta.className = "mt-0.5 text-xs text-panel-muted";
                meta.textContent = formatBytes(file.size);
                copy.append(name, meta);

                const removeButton = document.createElement("button");
                removeButton.type = "button";
                removeButton.className = "px-2 py-1 text-sm font-semibold text-rose transition hover:text-rose/70";
                removeButton.textContent = "Remove";
                removeButton.addEventListener("click", () => removeFile(key));

                row.append(copy, removeButton);
                selectedList.append(row);
            });

            addMoreButton.textContent = files.length > 0 ? "Add more files" : "Choose files";
        };

        fileInput.addEventListener("change", () => {
            Array.from(fileInput.files || []).forEach((file) => {
                const key = fileKey(file);
                if (!selectedFiles.has(key)) {
                    selectedFiles.set(key, file);
                }
            });
            syncInputFiles();
            renderSelectedFiles();
        });

        addMoreButton.addEventListener("click", () => fileInput.click());
        renderSelectedFiles();
    });
};

initImportUpload();
