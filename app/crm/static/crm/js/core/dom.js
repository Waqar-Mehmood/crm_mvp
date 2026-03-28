const createMessage = (text, className, datasetKey) => {
    const message = document.createElement("p");
    message.className = className;
    if (datasetKey) {
        message.dataset[datasetKey] = "true";
    }
    message.textContent = text;
    return message;
};

export const createMutedMessage = (text, datasetKey) => createMessage(
    text,
    "m-0 text-sm leading-6 text-brand-text-muted",
    datasetKey,
);

export const createNoticeMessage = (text) => createMessage(
    text,
    "tw-notice border-rose/30 bg-rose/10",
);
