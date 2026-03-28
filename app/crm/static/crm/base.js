document.querySelectorAll("[data-column-picker-form]").forEach((form) => {
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

if (!window.matchMedia("(prefers-reduced-motion: reduce)").matches) {
    document.querySelectorAll("[data-animated-disclosure]").forEach((disclosure) => {
        const summary = disclosure.querySelector("summary");
        const body = disclosure.querySelector("[data-disclosure-body]");

        if (!summary || !body) {
            return;
        }

        const setClosedState = () => {
            body.style.height = "0px";
            body.style.opacity = "0";
        };

        const setOpenState = () => {
            body.style.height = "auto";
            body.style.opacity = "1";
        };

        if (disclosure.open) {
            setOpenState();
        } else {
            setClosedState();
        }

        summary.addEventListener("click", (event) => {
            event.preventDefault();

            if (disclosure.dataset.animating === "true") {
                return;
            }

            const isOpening = !disclosure.open;
            disclosure.dataset.animating = "true";

            if (isOpening) {
                disclosure.open = true;
                setClosedState();
                requestAnimationFrame(() => {
                    body.style.height = `${body.scrollHeight}px`;
                    body.style.opacity = "1";
                });
            } else {
                body.style.height = `${body.offsetHeight}px`;
                body.style.opacity = "1";
                requestAnimationFrame(() => {
                    setClosedState();
                });
            }

            const handleTransitionEnd = (transitionEvent) => {
                if (
                    transitionEvent.target !== body
                    || transitionEvent.propertyName !== "height"
                ) {
                    return;
                }

                body.removeEventListener("transitionend", handleTransitionEnd);

                if (isOpening) {
                    setOpenState();
                } else {
                    disclosure.open = false;
                    setClosedState();
                }

                disclosure.removeAttribute("data-animating");
            };

            body.addEventListener("transitionend", handleTransitionEnd);
        });
    });
}

const initializeDevLiveReload = () => {
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

initializeDevLiveReload();
