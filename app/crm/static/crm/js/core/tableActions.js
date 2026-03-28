const decodeBase64Utf8 = (value) => {
    if (!value) {
        return "";
    }

    const bytes = Uint8Array.from(window.atob(value), (char) => char.charCodeAt(0));
    return new TextDecoder().decode(bytes);
};

const copyText = async (value) => {
    if (navigator.clipboard?.writeText) {
        await navigator.clipboard.writeText(value);
        return;
    }

    const fallback = document.createElement("textarea");
    fallback.value = value;
    fallback.setAttribute("readonly", "readonly");
    fallback.className = "sr-only";
    document.body.appendChild(fallback);
    fallback.select();
    document.execCommand("copy");
    fallback.remove();
};

const handleCopyAction = async (trigger) => {
    const text = decodeBase64Utf8(trigger.dataset.copyBase64 || "");
    if (!text) {
        return;
    }

    await copyText(text);
};

const updateStatusBadge = (trigger) => {
    const targetId = trigger.dataset.statusTargetId;
    if (!targetId) {
        return;
    }

    const badge = document.getElementById(targetId);
    if (!badge) {
        return;
    }

    const tone = trigger.dataset.statusTone || "review";
    const classKey = tone === "suggested" ? "statusClassSuggested" : "statusClassReview";
    const nextClassName = badge.dataset[classKey];
    if (nextClassName) {
        badge.className = nextClassName;
    }
    if (trigger.dataset.statusLabel) {
        badge.textContent = trigger.dataset.statusLabel;
    }
};

const handleSetSelectValue = (trigger) => {
    const targetId = trigger.dataset.targetId;
    if (!targetId) {
        return;
    }

    const field = document.getElementById(targetId);
    if (!field) {
        return;
    }

    field.value = trigger.dataset.targetValue || "";
    field.dispatchEvent(new Event("change", { bubbles: true }));
    updateStatusBadge(trigger);
};

export const initTableActions = (root = document) => {
    root.addEventListener("click", (event) => {
        const trigger = event.target.closest("[data-table-action]");
        if (!trigger) {
            return;
        }

        const action = trigger.dataset.tableAction;
        if (!action) {
            return;
        }

        event.preventDefault();

        if (action === "copy-base64") {
            handleCopyAction(trigger).catch(() => {});
            return;
        }

        if (action === "set-select-value") {
            handleSetSelectValue(trigger);
        }
    });
};
