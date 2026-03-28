export const initDevLiveReload = () => {
    const body = document.body;
    if (!body || body.dataset.devReloadEnabled !== "true") {
        return;
    }

    const reloadUrl = body.dataset.devReloadUrl;
    const intervalMs = Number.parseInt(body.dataset.devReloadInterval || "1500", 10);
    if (!reloadUrl || !Number.isFinite(intervalMs) || intervalMs <= 0) {
        return;
    }

    let baselineToken = null;
    let intervalId = null;
    let pollInFlight = false;
    let stopped = false;

    const stopPolling = () => {
        stopped = true;
        if (intervalId !== null) {
            window.clearInterval(intervalId);
            intervalId = null;
        }
    };

    const fetchToken = async () => {
        try {
            const response = await fetch(reloadUrl, {
                cache: "no-store",
                credentials: "same-origin",
            });
            if (!response.ok) {
                return null;
            }

            const payload = await response.json();
            return typeof payload.token === "string" ? payload.token : null;
        } catch (_error) {
            return null;
        }
    };

    const pollForChanges = async () => {
        if (stopped || pollInFlight || document.visibilityState === "hidden") {
            return;
        }

        pollInFlight = true;

        try {
            const nextToken = await fetchToken();
            if (!nextToken) {
                return;
            }

            if (baselineToken === null) {
                baselineToken = nextToken;
                return;
            }

            if (nextToken !== baselineToken) {
                stopPolling();
                window.location.reload();
            }
        } finally {
            pollInFlight = false;
        }
    };

    document.addEventListener("visibilitychange", () => {
        if (document.visibilityState === "visible") {
            void pollForChanges();
        }
    });
    window.addEventListener("beforeunload", stopPolling, { once: true });

    void pollForChanges();
    intervalId = window.setInterval(() => {
        void pollForChanges();
    }, intervalMs);
};
