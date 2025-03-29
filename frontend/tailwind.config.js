/** @type {import('tailwindcss').Config} */
module.exports = {
  content: [
    "./src/**/*.{js,jsx,ts,tsx}",
  ],
  darkMode: "class", // Enable dark mode with the "dark" class
  theme: {
    extend: {
      colors: {
        // New palette for both Home.js and MetadataViewer.js
        "midnight-blue": "#1A1D2E",
        "frost-white": "#F5F7FA",
        "glacier-blue": "#4A90E2",
        "slate-teal": "#3B6978",
        "mist-gray": "#A3BFFA",
      },
      fontFamily: {
        inter: ["Inter", "sans-serif"],
      },
    },
  },
  plugins: [],
};