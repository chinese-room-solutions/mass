/** @type {import('tailwindcss').Config} */
module.exports = {
  content: [
    "../internal/web/templates/**/*.templ",
    "../internal/web/templates/**/*.go",
    "../internal/web/**/*.go",
  ],
  darkMode: "class",
  theme: {
    extend: {},
  },
  plugins: [],
};
