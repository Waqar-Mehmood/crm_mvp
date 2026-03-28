export const initImportDetailAutoRefresh = (root = document) => {
    root.querySelectorAll("[data-import-auto-refresh]").forEach((node) => {
        const intervalMs = Number.parseInt(
            node.dataset.importAutoRefreshInterval || "4000",
            10,
        );
        if (!Number.isFinite(intervalMs) || intervalMs <= 0) {
            return;
        }

        window.setTimeout(() => window.location.reload(), intervalMs);
    });
};

initImportDetailAutoRefresh();
