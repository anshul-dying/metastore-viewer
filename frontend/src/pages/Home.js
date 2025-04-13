import React, { useEffect } from "react";
import { motion, useAnimation } from "framer-motion";
import { FaLinkedin, FaGithub, FaDatabase, FaChartBar, FaHistory, FaSearch, FaArrowDown } from "react-icons/fa";
import Lottie from "lottie-react";
import MetadataViewer from "../components/MetadataViewer";
import './Home.css';

// Import a Lottie animation JSON (you can download one from lottiefiles.com, e.g., a data visualization animation)
import dataAnimation from "../assets/data-visualition.json"; // Placeholder: You'll need to add this file

const Home = ({ darkMode, setDarkMode }) => {
  const controls = useAnimation();

  useEffect(() => {
    controls.start({
      y: [0, -10, 0],
      transition: {
        duration: 2,
        repeat: Infinity,
        repeatType: "loop"
      }
    });
  }, [controls]);

  // Feature data for cards
  const features = [
    {
      icon: <FaDatabase className="text-5xl text-accent-blue" />,
      title: "Browse Schema & Metadata",
      desc: "Dive into table schemas and metadata with ease.",
    },
    {
      icon: <FaChartBar className="text-5xl text-green-500" />,
      title: "Visualize Snapshots & Evolution",
      desc: "Track table history and changes visually.",
    },
    {
      icon: <FaHistory className="text-5xl text-purple-500" />,
      title: "Compare Versions",
      desc: "Compare table versions side-by-side.",
    },
    {
      icon: <FaSearch className="text-5xl text-yellow-500" />,
      title: "Run Queries with Trino",
      desc: "Query tables directly with Trino integration.",
    },
  ];

  // Mock recent searches (you can replace this with real data later)
  const recentSearches = [
    { path: "s3://test-bucket/sales-data", format: "Parquet", date: "2 hours ago" },
    { path: "s3://test-bucket/customer-analytics", format: "Delta", date: "Yesterday" },
    { path: "s3://test-bucket/product-inventory", format: "Iceberg", date: "3 days ago" },
  ];

  return (
    <div className={`min-h-screen transition-all duration-500 font-open-sans ${darkMode ? "bg-deep-teal text-white" : "bg-light-gray text-medium-gray"}`}>
      {/* Header */}
      <header className="fixed top-0 left-0 w-full z-50 transition-all duration-300">
        <div className="backdrop-blur-md bg-deep-teal/80 dark:bg-deep-teal/90 shadow-lg">
          <div className="max-w-7xl mx-auto px-6 py-4 flex flex-col md:flex-row justify-between items-center">
            <motion.h1
              initial={{ opacity: 0, x: -20 }}
              animate={{ opacity: 1, x: 0 }}
              transition={{ duration: 0.6 }}
              className="text-2xl font-montserrat font-bold bg-gradient-to-r from-accent-blue via-accent-green to-purple-500 text-transparent bg-clip-text"
            >
              Metastore Viewer
            </motion.h1>
            <nav className="flex mt-4 md:mt-0 space-x-8">
              <motion.a
                href="#home"
                className="nav-link"
                initial={{ opacity: 0, y: -10 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ duration: 0.6, delay: 0.1 }}
                whileHover={{ scale: 1.05 }}
              >
                Home
              </motion.a>
              <motion.a
                href="#features"
                className="nav-link"
                initial={{ opacity: 0, y: -10 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ duration: 0.6, delay: 0.2 }}
                whileHover={{ scale: 1.05 }}
              >
                Features
              </motion.a>
              <motion.a
                href="#metadata"
                className="nav-link"
                initial={{ opacity: 0, y: -10 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ duration: 0.6, delay: 0.3 }}
                whileHover={{ scale: 1.05 }}
              >
                Metadata
              </motion.a>
              <motion.a
                href="#recent"
                className="nav-link"
                initial={{ opacity: 0, y: -10 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ duration: 0.6, delay: 0.4 }}
                whileHover={{ scale: 1.05 }}
              >
                Recent
              </motion.a>
            </nav>
          </div>
        </div>
      </header>

      {/* Hero Section */}
      <section id="home" className="min-h-screen flex items-center justify-center overflow-hidden pt-24 pb-16 relative">
        {/* Background Elements */}
        <div className="absolute inset-0 hero-gradient-new"></div>
        <div className="absolute inset-0 hero-pattern opacity-10"></div>

        {/* Animated shapes */}
        <div className="animated-shape animated-shape-1"></div>
        <div className="animated-shape animated-shape-2"></div>
        <div className="animated-shape animated-shape-3"></div>

        <div className="max-w-7xl mx-auto px-6 z-10 grid grid-cols-1 lg:grid-cols-2 gap-12 items-center">
          <div className="text-left lg:order-1 order-2">
            <motion.div
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ duration: 0.8 }}
            >
              <h1 className="text-5xl md:text-6xl font-montserrat font-bold mb-6 hero-heading">
                <span className="text-white">Explore Lakehouse</span>
                <div className="bg-gradient-to-r from-pink-400 via-accent-green to-accent-blue text-transparent bg-clip-text mt-2">
                  Metadata
                </div>
              </h1>
            </motion.div>

            <motion.p
              className="text-xl md:text-2xl mb-10 text-white/80 max-w-xl"
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ duration: 0.8, delay: 0.2 }}
            >
              Visualize, compare, and explore Iceberg, Delta, Parquet, and Hudi tables with unprecedented clarity.
            </motion.p>

            <motion.div
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ duration: 0.8, delay: 0.4 }}
              className="flex flex-col sm:flex-row items-start gap-5"
            >
              <motion.a
                href="#metadata"
                className="hero-cta-button group"
                whileHover={{ scale: 1.05 }}
                whileTap={{ scale: 0.98 }}
              >
                <span className="relative z-10 flex items-center">
                  Get Started
                  <svg xmlns="http://www.w3.org/2000/svg" className="h-5 w-5 ml-2 group-hover:translate-x-1 transition-transform" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 7l5 5m0 0l-5 5m5-5H6" />
                  </svg>
                </span>
              </motion.a>

              <motion.a
                href="#features"
                className="text-white font-medium text-lg flex items-center group hover:text-accent-green transition-colors"
                whileHover={{ x: 5 }}
              >
                Learn More
                <svg xmlns="http://www.w3.org/2000/svg" className="h-5 w-5 ml-1 group-hover:translate-x-1 transition-transform" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
                </svg>
              </motion.a>
            </motion.div>
          </div>

          <motion.div
            className="lg:order-2 order-1 flex justify-center"
            initial={{ opacity: 0, scale: 0.8 }}
            animate={{ opacity: 1, scale: 1 }}
            transition={{ duration: 0.8, delay: 0.1 }}
          >
            <div className="relative">
              <motion.div
                className="w-72 h-72 md:w-96 md:h-96 relative z-10 hero-image-container"
                animate={{
                  y: [0, -10, 0],
                }}
                transition={{
                  duration: 5,
                  repeat: Infinity,
                  repeatType: "reverse"
                }}
              >
                <Lottie
                  animationData={dataAnimation}
                  loop={true}
                  className="w-full h-full"
                />
                <div className="absolute inset-0 hero-image-glow rounded-full"></div>
              </motion.div>

              <div className="absolute top-0 left-0 right-0 bottom-0 bg-accent-blue/10 rounded-full filter blur-3xl -z-10"></div>
            </div>
          </motion.div>
        </div>

        <motion.div
          className="absolute bottom-10 left-1/2 transform -translate-x-1/2 text-white/50"
          animate={{
            y: [0, -10, 0]
          }}
          transition={{
            duration: 2,
            repeat: Infinity
          }}
        >
          <svg xmlns="http://www.w3.org/2000/svg" className="h-8 w-8" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 14l-7 7m0 0l-7-7m7 7V3" />
          </svg>
        </motion.div>
      </section>

      {/* Floating Dark Mode Toggle at Bottom-Left */}
      <motion.div
        className="fixed bottom-6 left-6 z-50"
        initial={{ opacity: 0, scale: 0.8 }}
        animate={{ opacity: 1, scale: 1 }}
        transition={{ duration: 0.6, delay: 1 }}
      >
        <div className="p-3 rounded-full glass-card enhanced-shadow">
          <label className="relative inline-flex items-center cursor-pointer">
            <input
              type="checkbox"
              className="sr-only peer"
              checked={darkMode}
              onChange={() => setDarkMode(!darkMode)}
            />
            <div className="w-14 h-7 rounded-full peer dark:bg-medium-gray/50 peer-checked:after:translate-x-7 after:content-[''] after:absolute after:top-0.5 after:left-0.5 after:bg-white after:rounded-full after:h-6 after:w-6 after:transition-all peer-checked:bg-accent-blue"></div>
            <span className="ml-2 text-sm text-subtle-gray dark:text-white/70">{darkMode ? 'Dark' : 'Light'}</span>
          </label>
        </div>
      </motion.div>

      {/* Features Section */}
      <motion.section
        id="features"
        className="py-20 px-6 relative overflow-hidden"
        initial={{ opacity: 0 }}
        whileInView={{ opacity: 1 }}
        transition={{ duration: 0.8 }}
        viewport={{ once: true, margin: "-100px" }}
      >
        <div className="absolute inset-0 grid-background"></div>
        <div className="max-w-7xl mx-auto relative">
          <motion.h2
            initial={{ opacity: 0, y: 20 }}
            whileInView={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.6 }}
            viewport={{ once: true }}
            className="text-4xl md:text-5xl font-montserrat font-bold text-center mb-4 text-gradient"
          >
            Why Choose Metastore Viewer?
          </motion.h2>
          <motion.p
            initial={{ opacity: 0, y: 20 }}
            whileInView={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.6, delay: 0.2 }}
            viewport={{ once: true }}
            className="text-center text-medium-gray/70 dark:text-white/70 text-xl max-w-3xl mx-auto mb-16"
          >
            Our platform offers powerful features designed to make data exploration intuitive and efficient
          </motion.p>
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-8">
            {features.map((feature, index) => (
              <motion.div
                key={index}
                className="feature-card glass-card enhanced-shadow"
                initial={{ opacity: 0, y: 40 }}
                whileInView={{ opacity: 1, y: 0 }}
                transition={{ duration: 0.6, delay: index * 0.1 }}
                whileHover={{
                  y: -10,
                  boxShadow: "0 20px 40px rgba(0,0,0,0.2)"
                }}
                viewport={{ once: true }}
              >
                <div className="feature-icon-container mb-6">
                  <div className="feature-icon-bg"></div>
                  {feature.icon}
                </div>
                <h3 className="text-xl font-montserrat font-semibold mb-3">{feature.title}</h3>
                <p className="text-medium-gray/70 dark:text-white/70">{feature.desc}</p>
              </motion.div>
            ))}
          </div>
        </div>
      </motion.section>

      {/* Metadata Viewer Section */}
      <motion.section
        id="metadata"
        className="py-20 px-6 bg-gradient-to-b from-transparent to-accent-blue/5 dark:to-accent-blue/10"
        initial={{ opacity: 0 }}
        whileInView={{ opacity: 1 }}
        transition={{ duration: 0.8 }}
        viewport={{ once: true, margin: "-100px" }}
      >
        <div className="max-w-7xl mx-auto">
          <motion.h2
            initial={{ opacity: 0, y: 20 }}
            whileInView={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.6 }}
            viewport={{ once: true }}
            className="text-4xl md:text-5xl font-montserrat font-bold text-center mb-4 text-gradient"
          >
            Explore Your Metadata
          </motion.h2>
          <motion.p
            initial={{ opacity: 0, y: 20 }}
            whileInView={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.6, delay: 0.2 }}
            viewport={{ once: true }}
            className="text-center text-medium-gray/70 dark:text-white/70 text-xl max-w-3xl mx-auto mb-16"
          >
            Powerful visual tools to navigate and understand your data assets
          </motion.p>
          <motion.div
            initial={{ opacity: 0, y: 20 }}
            whileInView={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.6, delay: 0.3 }}
            viewport={{ once: true }}
            className="glass-card enhanced-shadow rounded-2xl p-2 border border-white/10"
          >
            <MetadataViewer darkMode={darkMode} />
          </motion.div>
        </div>
      </motion.section>

      {/* Recent Searches Section */}
      <motion.section
        id="recent"
        className="py-20 px-6 relative overflow-hidden"
        initial={{ opacity: 0 }}
        whileInView={{ opacity: 1 }}
        transition={{ duration: 0.8 }}
        viewport={{ once: true, margin: "-100px" }}
      >
        <div className="max-w-7xl mx-auto">
          <motion.h2
            initial={{ opacity: 0, y: 20 }}
            whileInView={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.6 }}
            viewport={{ once: true }}
            className="text-4xl md:text-5xl font-montserrat font-bold text-center mb-4 text-gradient section-heading"
          >
            Recent Searches
          </motion.h2>
          <motion.p
            initial={{ opacity: 0, y: 20 }}
            whileInView={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.6, delay: 0.2 }}
            viewport={{ once: true }}
            className="text-center text-medium-gray/70 dark:text-white/70 text-xl max-w-3xl mx-auto mb-16"
          >
            Quickly access your recently explored data assets
          </motion.p>
          <div className="flex flex-wrap justify-center gap-6">
            {recentSearches.map((search, index) => (
              <motion.div
                key={index}
                className="recent-search-card"
                initial={{ opacity: 0, scale: 0.9 }}
                whileInView={{ opacity: 1, scale: 1 }}
                transition={{ duration: 0.6, delay: index * 0.1 }}
                whileHover={{ scale: 1.05, y: -5 }}
                viewport={{ once: true }}
              >
                <div className="search-format-badge">
                  {search.format}
                </div>
                <h3 className="font-montserrat font-semibold text-lg mb-2 text-medium-gray dark:text-white truncate max-w-full">
                  {search.path}
                </h3>
                <div className="flex justify-between items-center mt-3">
                  <p className="text-sm text-medium-gray/60 dark:text-white/60">{search.date}</p>
                  <motion.button
                    whileHover={{ scale: 1.1 }}
                    whileTap={{ scale: 0.9 }}
                    className="text-accent-blue hover:text-blue-600 transition-colors"
                  >
                    <FaSearch />
                  </motion.button>
                </div>
              </motion.div>
            ))}
          </div>
        </div>
      </motion.section>

      {/* Call to Action Section */}
      <motion.section
        className="py-20 px-6 relative overflow-hidden"
        initial={{ opacity: 0 }}
        whileInView={{ opacity: 1 }}
        transition={{ duration: 0.8 }}
        viewport={{ once: true, margin: "-100px" }}
      >
        <div className="absolute inset-0 bg-gradient-to-b from-accent-blue/10 to-green-500/10"></div>
        <div className="absolute inset-0 hero-pattern opacity-[0.03]"></div>
        <div className="max-w-7xl mx-auto relative z-10">
          <motion.div
            className="cta-card"
            initial={{ opacity: 0, y: 20 }}
            whileInView={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.6 }}
            viewport={{ once: true }}
          >
            <motion.div
              className="absolute -top-10 -right-10 w-40 h-40 bg-accent-green/10 rounded-full blur-xl"
              animate={{
                scale: [1, 1.2, 1],
                opacity: [0.5, 0.8, 0.5]
              }}
              transition={{
                duration: 4,
                repeat: Infinity,
                repeatType: "reverse"
              }}
            />
            <motion.div
              className="absolute -bottom-10 -left-10 w-40 h-40 bg-accent-blue/10 rounded-full blur-xl"
              animate={{
                scale: [1, 1.2, 1],
                opacity: [0.5, 0.8, 0.5]
              }}
              transition={{
                duration: 4,
                repeat: Infinity,
                repeatType: "reverse",
                delay: 1
              }}
            />

            <motion.h2
              className="text-3xl md:text-4xl font-montserrat font-bold mb-4 text-gradient"
              initial={{ opacity: 0, y: 10 }}
              whileInView={{ opacity: 1, y: 0 }}
              transition={{ duration: 0.6 }}
              viewport={{ once: true }}
            >
              Ready to Elevate Your Data Experience?
            </motion.h2>
            <motion.p
              className="text-medium-gray/70 dark:text-white/70 text-xl mb-8 max-w-2xl"
              initial={{ opacity: 0, y: 10 }}
              whileInView={{ opacity: 1, y: 0 }}
              transition={{ duration: 0.6, delay: 0.2 }}
              viewport={{ once: true }}
            >
              Start exploring your data assets with our powerful visualization tools
            </motion.p>
            <motion.div
              className="floating-animation"
              initial={{ opacity: 0, y: 10 }}
              whileInView={{ opacity: 1, y: 0 }}
              transition={{ duration: 0.6, delay: 0.4 }}
              viewport={{ once: true }}
            >
              <motion.a
                href="#metadata"
                className="primary-button group"
                whileHover={{
                  scale: 1.05,
                  boxShadow: "0 20px 40px rgba(34, 197, 94, 0.3)"
                }}
                whileTap={{ scale: 0.95 }}
              >
                <span className="relative z-10 flex items-center">
                  Get Started Now
                  <motion.svg
                    xmlns="http://www.w3.org/2000/svg"
                    className="h-5 w-5 ml-2 transition-transform group-hover:translate-x-1"
                    fill="none"
                    viewBox="0 0 24 24"
                    stroke="currentColor"
                  >
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M14 5l7 7m0 0l-7 7m7-7H3" />
                  </motion.svg>
                </span>
                <motion.span
                  className="absolute inset-0 bg-gradient-to-r from-accent-blue to-accent-green rounded-full z-0 opacity-90"
                  animate={{
                    background: ["linear-gradient(90deg, #00A1D6, #22c55e)", "linear-gradient(90deg, #22c55e, #00A1D6)", "linear-gradient(90deg, #00A1D6, #22c55e)"],
                  }}
                  transition={{
                    duration: 3,
                    repeat: Infinity,
                    repeatType: "reverse"
                  }}
                />
              </motion.a>
            </motion.div>
          </motion.div>
        </div>
      </motion.section>

      {/* Footer */}
      <footer className="py-12 bg-deep-teal/90 backdrop-blur-md text-white relative overflow-hidden">
        <div className="absolute inset-0 hero-pattern opacity-[0.02]"></div>
        <div className="max-w-7xl mx-auto px-6 relative z-10">
          <div className="grid grid-cols-1 md:grid-cols-3 gap-8 mb-8">
            <div>
              <h3 className="text-xl font-montserrat font-bold mb-4 bg-gradient-to-r from-accent-blue to-accent-green text-transparent bg-clip-text">Metastore Viewer</h3>
              <p className="text-white/70 max-w-xs">
                The ultimate solution for visualizing and exploring your data lake metadata.
              </p>
            </div>
            <div>
              <h4 className="text-lg font-montserrat font-semibold mb-4">Quick Links</h4>
              <ul className="space-y-2">
                <li><a href="#home" className="text-white/70 hover:text-accent-blue transition-colors">Home</a></li>
                <li><a href="#features" className="text-white/70 hover:text-accent-blue transition-colors">Features</a></li>
                <li><a href="#metadata" className="text-white/70 hover:text-accent-blue transition-colors">Metadata Explorer</a></li>
                <li><a href="#recent" className="text-white/70 hover:text-accent-blue transition-colors">Recent Searches</a></li>
              </ul>
            </div>
            <div>
              <h4 className="text-lg font-montserrat font-semibold mb-4">Connect With Us</h4>
              <div className="flex space-x-4">
                <motion.a
                  href="https://www.linkedin.com/in/anshul-khaire-77732922a/"
                  target="_blank"
                  rel="noopener noreferrer"
                  whileHover={{ scale: 1.2, rotate: 10 }}
                  className="w-10 h-10 rounded-full bg-white/10 flex items-center justify-center hover:bg-accent-blue/80 transition-colors"
                >
                  <FaLinkedin className="text-white text-xl" />
                </motion.a>
                <motion.a
                  href="https://github.com/anshul-dying"
                  target="_blank"
                  rel="noopener noreferrer"
                  whileHover={{ scale: 1.2, rotate: -10 }}
                  className="w-10 h-10 rounded-full bg-white/10 flex items-center justify-center hover:bg-accent-blue/80 transition-colors"
                >
                  <FaGithub className="text-white text-xl" />
                </motion.a>
              </div>
            </div>
          </div>
          <div className="pt-8 border-t border-white/10 text-center text-white/50 text-sm">
            <p>© 2025 Metastore Viewer. All rights reserved.</p>
            <p className="mt-2">Designed with ❤️ for the National Level Hackathon</p>
          </div>
        </div>
      </footer>
    </div>
  );
};

export default Home;