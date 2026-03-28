const MENU_SELECTOR = "[data-dropdown-menu]";
const GROUP_SELECTOR = "[data-dropdown-group]";

const isDetailsElement = (element) => element instanceof HTMLDetailsElement;

const closeMenu = (menu) => {
    if (!isDetailsElement(menu) || !menu.open) {
        return;
    }

    menu.open = false;
};

const getGroupMenus = (menu) => {
    const group = menu.closest(GROUP_SELECTOR);

    if (!group) {
        return Array.from(document.querySelectorAll(MENU_SELECTOR));
    }

    return Array.from(group.querySelectorAll(MENU_SELECTOR));
};

const closeSiblingMenus = (menu) => {
    getGroupMenus(menu).forEach((candidate) => {
        if (candidate !== menu) {
            closeMenu(candidate);
        }
    });
};

const closeMenusOutsideTarget = (target) => {
    if (!(target instanceof Node)) {
        return;
    }

    document.querySelectorAll(`${MENU_SELECTOR}[open]`).forEach((menu) => {
        if (!menu.contains(target)) {
            closeMenu(menu);
        }
    });
};

export const initDropdownMenus = (root = document) => {
    root.querySelectorAll(MENU_SELECTOR).forEach((menu) => {
        if (!isDetailsElement(menu) || menu.dataset.dropdownMenuInitialized === "true") {
            return;
        }

        menu.dataset.dropdownMenuInitialized = "true";

        menu.addEventListener("toggle", () => {
            if (menu.open) {
                closeSiblingMenus(menu);
            }
        });
    });

    if (document.documentElement.dataset.dropdownMenusBound === "true") {
        return;
    }

    document.documentElement.dataset.dropdownMenusBound = "true";

    document.addEventListener("pointerdown", (event) => {
        closeMenusOutsideTarget(event.target);
    });

    document.addEventListener("keydown", (event) => {
        if (event.key !== "Escape") {
            return;
        }

        const openMenus = Array.from(document.querySelectorAll(`${MENU_SELECTOR}[open]`));

        if (!openMenus.length) {
            return;
        }

        const lastOpenMenu = openMenus[openMenus.length - 1];
        openMenus.forEach(closeMenu);

        const summary = lastOpenMenu.querySelector("summary");
        if (summary instanceof HTMLElement) {
            summary.focus();
        }
    });
};
