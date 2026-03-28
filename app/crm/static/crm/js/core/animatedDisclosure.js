export const initAnimatedDisclosures = (root = document) => {
    if (window.matchMedia("(prefers-reduced-motion: reduce)").matches) {
        return;
    }

    root.querySelectorAll("[data-animated-disclosure]").forEach((disclosure) => {
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
};
