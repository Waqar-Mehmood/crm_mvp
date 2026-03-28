const path = require("path");

const projectRoot = process.env.TAILWIND_PROJECT_ROOT
  ? path.resolve(process.env.TAILWIND_PROJECT_ROOT)
  : path.resolve(__dirname, "app");

const withOpacity = (cssVariable) => `rgb(var(${cssVariable}) / <alpha-value>)`;

module.exports = {
  content: [
    path.join(projectRoot, "crm/templates/**/*.html"),
    path.join(projectRoot, "crm/**/*.py"),
  ],
  corePlugins: {
    preflight: false,
  },
  theme: {
    extend: {
      colors: {
        ink: {
          950: withOpacity("--crm-brand-shell-950"),
          900: withOpacity("--crm-brand-shell-900"),
          800: withOpacity("--crm-brand-shell-800"),
        },
        paper: {
          100: withOpacity("--crm-brand-surface-base"),
          200: withOpacity("--crm-brand-surface-border-soft"),
        },
        panel: {
          text: withOpacity("--crm-brand-text-base"),
          muted: withOpacity("--crm-brand-text-muted"),
        },
        accent: {
          DEFAULT: withOpacity("--crm-brand-accent"),
          strong: withOpacity("--crm-brand-accent-strong"),
        },
        teal: withOpacity("--crm-brand-teal"),
        rose: withOpacity("--crm-brand-danger"),
        brand: {
          shell: {
            950: withOpacity("--crm-brand-shell-950"),
            900: withOpacity("--crm-brand-shell-900"),
            800: withOpacity("--crm-brand-shell-800"),
          },
          surface: {
            base: withOpacity("--crm-brand-surface-base"),
            warm: withOpacity("--crm-brand-surface-warm"),
            soft: withOpacity("--crm-brand-surface-soft"),
            border: withOpacity("--crm-brand-surface-border"),
            borderSoft: withOpacity("--crm-brand-surface-border-soft"),
            chrome: withOpacity("--crm-brand-surface-chrome"),
          },
          text: {
            base: withOpacity("--crm-brand-text-base"),
            strong: withOpacity("--crm-brand-text-strong"),
            heading: withOpacity("--crm-brand-text-heading"),
            soft: withOpacity("--crm-brand-text-soft"),
            muted: withOpacity("--crm-brand-text-muted"),
            subtle: withOpacity("--crm-brand-text-subtle"),
            inverse: withOpacity("--crm-brand-text-inverse"),
          },
          accent: {
            DEFAULT: withOpacity("--crm-brand-accent"),
            strong: withOpacity("--crm-brand-accent-strong"),
            soft: withOpacity("--crm-brand-accent-soft"),
            start: withOpacity("--crm-brand-accent-start"),
            end: withOpacity("--crm-brand-accent-end"),
            glow: withOpacity("--crm-brand-accent-glow"),
          },
          teal: withOpacity("--crm-brand-teal"),
          danger: withOpacity("--crm-brand-danger"),
          chip: {
            workBg: withOpacity("--crm-brand-chip-work-bg"),
            workBorder: withOpacity("--crm-brand-chip-work-border"),
            workText: withOpacity("--crm-brand-chip-work-text"),
            personalBg: withOpacity("--crm-brand-chip-personal-bg"),
            personalBorder: withOpacity("--crm-brand-chip-personal-border"),
            personalText: withOpacity("--crm-brand-chip-personal-text"),
            neutralBg: withOpacity("--crm-brand-chip-neutral-bg"),
            neutralBorder: withOpacity("--crm-brand-chip-neutral-border"),
            neutralText: withOpacity("--crm-brand-chip-neutral-text"),
          },
        },
      },
      fontFamily: {
        sans: ["Avenir Next", "Segoe UI", "Trebuchet MS", "sans-serif"],
        serif: [
          "Iowan Old Style",
          "Palatino Linotype",
          "Book Antiqua",
          "URW Palladio L",
          "serif",
        ],
      },
      borderRadius: {
        "crm-lg": "28px",
        "crm-md": "20px",
        "crm-sm": "14px",
      },
      boxShadow: {
        crm: "var(--crm-shadow-crm)",
        "crm-soft": "var(--crm-shadow-crm-soft)",
        "brand-card": "var(--crm-shadow-brand-card)",
        "brand-panel": "var(--crm-shadow-brand-panel)",
        "brand-button": "var(--crm-shadow-brand-button)",
        "brand-button-strong": "var(--crm-shadow-brand-button-strong)",
        "brand-dark-inset": "var(--crm-shadow-brand-dark-inset)",
        "brand-surface-inset": "var(--crm-shadow-brand-surface-inset)",
        "brand-surface-inset-strong": "var(--crm-shadow-brand-surface-inset-strong)",
        "brand-focus": "var(--crm-shadow-brand-focus)",
        "brand-dropdown": "var(--crm-shadow-brand-dropdown)",
        "brand-input-soft": "var(--crm-shadow-brand-input-soft)",
        "brand-popover": "var(--crm-shadow-brand-popover)",
      },
      backgroundImage: {
        "crm-shell":
          "radial-gradient(circle at top left, rgb(var(--crm-brand-teal) / 0.28), transparent 34%), radial-gradient(circle at top right, rgb(var(--crm-brand-accent) / 0.18), transparent 26%), linear-gradient(140deg, rgb(var(--crm-brand-shell-950)) 0%, rgb(var(--crm-brand-shell-900)) 45%, rgb(var(--crm-brand-shell-800)) 100%)",
        "brand-gradient-shell":
          "radial-gradient(circle at top left, rgb(var(--crm-brand-teal) / 0.28), transparent 34%), radial-gradient(circle at top right, rgb(var(--crm-brand-accent) / 0.18), transparent 26%), linear-gradient(140deg, rgb(var(--crm-brand-shell-950)) 0%, rgb(var(--crm-brand-shell-900)) 45%, rgb(var(--crm-brand-shell-800)) 100%)",
        "brand-gradient-hero":
          "linear-gradient(135deg, rgb(255 255 255 / 0.05), rgb(255 255 255 / 0.02)), linear-gradient(130deg, rgb(var(--crm-brand-teal) / 0.18), rgb(var(--crm-brand-accent) / 0.12))",
        "brand-gradient-surface":
          "linear-gradient(180deg, rgb(var(--crm-brand-surface-base) / 0.98), rgb(var(--crm-brand-surface-warm) / 0.94))",
        "brand-gradient-accent":
          "linear-gradient(to right, rgb(var(--crm-brand-accent-start)), rgb(var(--crm-brand-accent-end)))",
        "brand-gradient-accent-soft":
          "linear-gradient(to right, rgb(var(--crm-brand-accent-soft)), rgb(var(--crm-brand-accent-end)))",
      },
    },
  },
};
