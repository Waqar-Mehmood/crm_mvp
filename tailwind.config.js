const path = require("path");

const projectRoot = process.env.TAILWIND_PROJECT_ROOT
  ? path.resolve(process.env.TAILWIND_PROJECT_ROOT)
  : path.resolve(__dirname, "app");

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
          950: "#08131e",
          900: "#102031",
          800: "#183149",
        },
        paper: {
          100: "#f7f1e3",
          200: "#eadfca",
        },
        panel: {
          text: "#142233",
          muted: "#5e6b77",
        },
        accent: {
          DEFAULT: "#d17d2f",
          strong: "#b85f17",
        },
        teal: "#1d8b8a",
        rose: "#d36e5f",
        brand: {
          shell: {
            950: "#08131e",
            900: "#102031",
            800: "#13273f",
          },
          surface: {
            base: "#f7f1e3",
            warm: "#efe5d0",
            soft: "#fffdf8",
            border: "#ddcfb6",
            borderSoft: "#eadfca",
            chrome: "#f4ebd7",
          },
          text: {
            base: "#142233",
            strong: "#112033",
            heading: "#4a5568",
            soft: "#415268",
            muted: "#5e6b77",
            subtle: "#657486",
            inverse: "#f7f1e3",
          },
          accent: {
            DEFAULT: "#d17d2f",
            strong: "#b85f17",
            soft: "#f3d4ab",
            start: "#f5c994",
            end: "#ee8e42",
            glow: "#f7b067",
          },
          teal: "#1d8b8a",
          danger: "#d36e5f",
          chip: {
            workBg: "#e0f2ec",
            workBorder: "#b8ddd4",
            workText: "#0d766d",
            personalBg: "#edf7f1",
            personalBorder: "#cee5d7",
            personalText: "#2f7a61",
            neutralBg: "#eef3f5",
            neutralBorder: "#d8e1e6",
            neutralText: "#5a6b78",
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
        crm: "0 28px 80px rgba(3, 9, 17, 0.28)",
        "crm-soft": "0 20px 40px rgba(6, 14, 24, 0.16)",
        "brand-card": "0 22px 56px rgba(15, 23, 42, 0.14)",
        "brand-panel": "0 18px 38px rgba(15, 23, 42, 0.12)",
        "brand-button": "0 12px 24px rgba(209, 125, 47, 0.16)",
        "brand-button-strong": "0 12px 28px rgba(209, 125, 47, 0.20)",
        "brand-dark-inset": "inset 0 1px 0 rgba(255, 255, 255, 0.04)",
        "brand-surface-inset": "inset 0 1px 0 rgba(255, 255, 255, 0.45)",
        "brand-surface-inset-strong": "inset 0 1px 0 rgba(255, 255, 255, 0.78)",
      },
      backgroundImage: {
        "crm-shell":
          "radial-gradient(circle at top left, rgba(29, 139, 138, 0.28), transparent 34%), radial-gradient(circle at top right, rgba(209, 125, 47, 0.18), transparent 26%), linear-gradient(140deg, #08131e 0%, #102031 45%, #13273f 100%)",
        "brand-gradient-shell":
          "radial-gradient(circle at top left, rgba(29, 139, 138, 0.28), transparent 34%), radial-gradient(circle at top right, rgba(209, 125, 47, 0.18), transparent 26%), linear-gradient(140deg, #08131e 0%, #102031 45%, #13273f 100%)",
        "brand-gradient-hero":
          "linear-gradient(135deg, rgba(255, 255, 255, 0.05), rgba(255, 255, 255, 0.02)), linear-gradient(130deg, rgba(29, 139, 138, 0.18), rgba(209, 125, 47, 0.12))",
        "brand-gradient-surface":
          "linear-gradient(180deg, rgba(247, 241, 227, 0.98), rgba(241, 233, 218, 0.94))",
        "brand-gradient-accent":
          "linear-gradient(to right, #f5c994, #ee8e42)",
        "brand-gradient-accent-soft":
          "linear-gradient(to right, #f3d4ab, #ee8e42)",
      },
    },
  },
};
