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
      },
      backgroundImage: {
        "crm-shell":
          "radial-gradient(circle at top left, rgba(29, 139, 138, 0.28), transparent 34%), radial-gradient(circle at top right, rgba(209, 125, 47, 0.18), transparent 26%), linear-gradient(140deg, #08131e 0%, #102031 45%, #13273f 100%)",
      },
    },
  },
};
