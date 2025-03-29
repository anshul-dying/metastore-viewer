import React from "react";
import { motion } from "framer-motion";
import { FaLinkedin, FaGithub, FaDatabase, FaChartBar, FaHistory, FaSearch } from "react-icons/fa";
import Lottie from "lottie-react";
import MetadataViewer from "../components/MetadataViewer";

// Import a Lottie animation JSON (you can download one from lottiefiles.com, e.g., a data visualization animation)
import dataAnimation from "../assets/data-visualition.json"; // Placeholder: You'll need to add this file

const Home = ({ darkMode, setDarkMode }) => {
  // Feature data for cards
  const features = [
    {
      icon: <FaDatabase className="text-4xl text-medium-gray dark:text-white" />,
      title: "Browse Schema & Metadata",
      desc: "Dive into table schemas and metadata with ease.",
    },
    {
      icon: <FaChartBar className="text-4xl text-medium-gray dark:text-white" />,
      title: "Visualize Snapshots & Evolution",
      desc: "Track table history and changes visually.",
    },
    {
      icon: <FaHistory className="text-4xl text-medium-gray dark:text-white" />,
      title: "Compare Versions",
      desc: "Compare table versions side-by-side.",
    },
    {
      icon: <FaSearch className="text-4xl text-medium-gray dark:text-white" />,
      title: "Run Queries with Trino",
      desc: "Query tables directly with Trino integration.",
    },
  ];

  // Mock recent searches (you can replace this with real data later)
  const recentSearches = [
    { path: "s3://test-bucket/data1", format: "Parquet" },
    { path: "s3://test-bucket/data2", format: "Delta" },
    { path: "s3://test-bucket/data3", format: "Iceberg" },
  ];

  return (
    <div className={`min-h-screen transition-all duration-500 font-open-sans ${darkMode ? "bg-deep-teal text-white" : "bg-light-gray text-medium-gray"}`}>
      {/* Header */}
      <header className="fixed top-0 left-0 w-full bg-deep-teal text-white z-50 shadow-md">
        <div className="max-w-6xl mx-auto px-4 py-4 flex justify-between items-center">
          <h1 className="text-2xl font-montserrat font-bold">Metastore Viewer</h1>
          <nav className="space-x-6">
            <a href="#home" className="hover:text-accent-blue transition-colors duration-300">Home</a>
            <a href="#features" className="hover:text-accent-blue transition-colors duration-300">Features</a>
            <a href="#metadata" className="hover:text-accent-blue transition-colors duration-300">Metadata</a>
            <a href="#recent" className="hover:text-accent-blue transition-colors duration-300">Recent Searches</a>
          </nav>
        </div>
      </header>

      {/* Hero Section */}
      <section id="home" className="min-h-screen flex items-center justify-center bg-gradient-to-br from-deep-teal to-accent-blue text-white relative overflow-hidden pt-16">
        {/* Animated Background */}
        <motion.div
          className="absolute inset-0 bg-accent-blue/20"
          animate={{ scale: [1, 1.1, 1] }}
          transition={{ duration: 5, repeat: Infinity, ease: "easeInOut" }}
        />

        <div className="text-center z-10 px-4">
          {/* Lottie Animation */}
          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.6 }}
            className="w-48 h-48 mx-auto mb-6"
          >
            <Lottie animationData={dataAnimation} loop={true} />
          </motion.div>

          <motion.h1
            className="text-hero font-montserrat font-bold mb-6"
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.6, delay: 0.2 }}
          >
            Explore Lakehouse Metadata Effortlessly
          </motion.h1>
          <motion.p
            className="text-xl mb-8 max-w-2xl mx-auto"
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.6, delay: 0.4 }}
          >
            Visualize, compare, and explore Iceberg, Delta, Parquet, and Hudi tables directly from your S3 or cloud storage without traditional metastores.
          </motion.p>
          <motion.a
            href="#metadata"
            className="inline-block px-6 py-3 bg-accent-blue text-white font-montserrat font-semibold rounded-md hover:bg-[#00B7F0] hover:scale-105 transition-all duration-300"
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.6, delay: 0.6 }}
          >
            Get Started
          </motion.a>
        </div>
      </section>

      {/* Floating Dark Mode Toggle at Bottom-Left */}
      <motion.div
        className="fixed bottom-6 left-6 z-50"
        initial={{ opacity: 0, y: 20 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.6 }}
      >
        <label className="relative inline-flex items-center cursor-pointer">
          <input
            type="checkbox"
            className="sr-only peer"
            checked={darkMode}
            onChange={() => setDarkMode(!darkMode)}
          />
          <div className="w-14 h-7 bg-subtle-gray peer-focus:ring-2 peer-focus:ring-accent-blue rounded-full peer dark:bg-medium-gray peer-checked:after:translate-x-7 peer-checked:after:border-white after:content-[''] after:absolute after:top-0.5 after:left-0.5 after:bg-white after:border after:rounded-full after:h-6 after:w-6 after:transition-all peer-checked:bg-accent-blue"></div>
        </label>
      </motion.div>

      {/* Metadata Viewer Section */}
      <motion.section
        id="metadata"
        className="py-16 px-4 max-w-6xl mx-auto"
        initial={{ opacity: 0, y: 20 }}
        whileInView={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.6 }}
        viewport={{ once: true }}
      >
        <h2 className="text-4xl font-montserrat font-bold text-center mb-12 text-medium-gray dark:text-white">
          Explore Your Metadata
        </h2>
        <div className="bg-white dark:bg-dark-teal rounded-lg shadow-md p-6 border border-subtle-gray dark:border-white/20">
          <MetadataViewer darkMode={darkMode} />
        </div>
      </motion.section>

      {/* Features Section */}
      <motion.section
        id="features"
        className="py-16 px-4 max-w-6xl mx-auto"
        initial={{ opacity: 0, y: 20 }}
        whileInView={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.6 }}
        viewport={{ once: true }}
      >
        <h2 className="text-4xl font-montserrat font-bold text-center mb-12 text-medium-gray dark:text-white">
          Why Choose Metastore Viewer?
        </h2>
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-8">
          {features.map((feature, index) => (
            <motion.div
              key={index}
              className="p-6 bg-white dark:bg-dark-teal rounded-lg shadow-md hover:shadow-lg transition-all duration-300 border border-subtle-gray dark:border-white/20"
              initial={{ opacity: 0, y: 20 }}
              whileInView={{ opacity: 1, y: 0 }}
              transition={{ duration: 0.6, delay: index * 0.2 }}
              whileHover={{ scale: 1.05 }}
              viewport={{ once: true }}
            >
              <div className="mb-4">{feature.icon}</div>
              <h3 className="text-xl font-montserrat font-semibold text-medium-gray dark:text-accent-blue mb-2">{feature.title}</h3>
              <p className="text-medium-gray dark:text-white">{feature.desc}</p>
            </motion.div>
          ))}
        </div>
      </motion.section>

      {/* Recent Searches Section */}
      <motion.section
        id="recent"
        className="py-16 px-4 max-w-6xl mx-auto"
        initial={{ opacity: 0, y: 20 }}
        whileInView={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.6 }}
        viewport={{ once: true }}
      >
        <h2 className="text-4xl font-montserrat font-bold text-center mb-12 text-medium-gray dark:text-white">
          Recent Searches
        </h2>
        <div className="flex overflow-x-auto space-x-4 pb-4">
          {recentSearches.map((search, index) => (
            <motion.div
              key={index}
              className="min-w-[250px] p-4 bg-white dark:bg-dark-teal rounded-lg shadow-md hover:shadow-lg transition-all duration-300 border border-subtle-gray dark:border-white/20"
              initial={{ opacity: 0, x: 20 }}
              whileInView={{ opacity: 1, x: 0 }}
              transition={{ duration: 0.6, delay: index * 0.2 }}
              whileHover={{ scale: 1.05 }}
              viewport={{ once: true }}
            >
              <h3 className="text-lg font-montserrat font-semibold text-medium-gray dark:text-accent-blue">{search.path}</h3>
              <p className="text-medium-gray dark:text-white">Format: {search.format}</p>
            </motion.div>
          ))}
        </div>
      </motion.section>

      {/* Footer */}
      <footer className="py-8 bg-deep-teal text-white text-center">
        <motion.div
          className="flex justify-center gap-6 mb-4"
          initial={{ opacity: 0 }}
          whileInView={{ opacity: 1 }}
          transition={{ duration: 0.6 }}
          viewport={{ once: true }}
        >
          <motion.a
            href="https://linkedin.com"
            target="_blank"
            whileHover={{ scale: 1.2 }}
            className="text-2xl"
          >
            <FaLinkedin />
          </motion.a>
          <motion.a
            href="https://github.com"
            target="_blank"
            whileHover={{ scale: 1.2 }}
            className="text-2xl"
          >
            <FaGithub />
          </motion.a>
        </motion.div>
        <p className="text-sm">© 2025 Metastore Viewer. All rights reserved.</p>
      </footer>
    </div>
  );
};

export default Home;