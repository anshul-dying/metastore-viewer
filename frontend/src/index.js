import React from "react";
import ReactDOM from "react-dom/client"; // Use "client" for createRoot
import App from "./App";
import "./index.css"; // Ensure this file exists
import Home from "./pages/Home";

const root = ReactDOM.createRoot(document.getElementById("root"));
root.render(
  <React.StrictMode>
    <App />
  </React.StrictMode>
);

export default Home;