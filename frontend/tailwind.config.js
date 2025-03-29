/** @type {import('tailwindcss').Config} */
module.exports = {
  content: [
    "./src/**/*.{js,jsx,ts,tsx}",
  ],
  darkMode: "class", // Enable dark mode with the "dark" class
  theme: {
    extend: {
      colors: {
        // Existing colors
        "deep-teal": "#1A2526",
        "light-gray": "#F5F5F5",
        "accent-blue": "#00A1D6",
        "medium-gray": "#4A4A4A",
        "subtle-gray": "#E0E0E0",
        // New color for dark mode boxes
        "dark-teal": "#2A3536", // Slightly lighter than deep-teal
      },
      fontFamily: {
        montserrat: ["Montserrat", "sans-serif"],
        "open-sans": ["Open Sans", "sans-serif"],
      },
      fontSize: {
        "hero": ["3rem", { lineHeight: "1.2" }],
        "subheading": ["1.5rem", { lineHeight: "1.4" }],
        "body": ["1rem", { lineHeight: "1.6" }],
        "small": ["0.875rem", { lineHeight: "1.5" }],
      },
    },
  },
  plugins: [],
};