import React, { useState, useEffect } from "react";
import Home from "./pages/Home";
import "./App.css";

const App = () => {
  const [darkMode, setDarkMode] = useState(false);

  // Apply the dark class to the root element based on darkMode state
  useEffect(() => {
    if (darkMode) {
      document.documentElement.classList.add("dark");
    } else {
      document.documentElement.classList.remove("dark");
    }
  }, [darkMode]);

  return (
    <div>
      <style>
        {`
          @import url('https://fonts.googleapis.com/css2?family=Montserrat:wght@600;700&family=Open+Sans:wght@400;600&display=swap');
        `}
      </style>
      <Home darkMode={darkMode} setDarkMode={setDarkMode} />
    </div>
  );
};

export default App;