import React, { useState, useEffect, useCallback, useMemo } from "react";
import axios from "axios";
import { FaFileAlt, FaFileExcel, FaFilePdf, FaFileImage, FaFileCode } from "react-icons/fa";
import { Bar } from "react-chartjs-2";
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  Title,
  Tooltip,
  Legend,
} from "chart.js";

// Register ChartJS components
ChartJS.register(CategoryScale, LinearScale, BarElement, Title, Tooltip, Legend);

class ErrorBoundary extends React.Component {
  state = { hasError: false };
  static getDerivedStateFromError() {
    return { hasError: true };
  }
  render() {
    if (this.state.hasError) {
      return <p className="text-red-500 text-center">Something went wrong. Please try again.</p>;
    }
    return this.props.children;
  }
}

const getFileIcon = (extension) => {
  const ext = extension.toLowerCase();
  if (["xls", "xlsx", "csv"].includes(ext)) return <FaFileExcel className="text-green-500" />;
  if (["pdf"].includes(ext)) return <FaFilePdf className="text-red-500" />;
  if (["jpg", "jpeg", "png", "gif"].includes(ext)) return <FaFileImage className="text-yellow-500" />;
  if (["parquet", "delta", "iceberg", "hudi"].includes(ext)) return <FaFileCode className="text-glacier-blue" />;
  return <FaFileAlt className="text-mist-gray dark:text-frost-white" />;
};

const debounce = (func, delay) => {
  let timeoutId;
  return (...args) => {
    clearTimeout(timeoutId);
    timeoutId = setTimeout(() => func(...args), delay);
  };
};

// Utility to format numbers with commas
const formatNumber = (num) => {
  return num.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
};

// Utility to format file size
const formatFileSize = (bytes) => {
  if (bytes === 0) return "0 Bytes";
  const k = 1024;
  const sizes = ["Bytes", "KB", "MB", "GB", "TB"];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + " " + sizes[i];
};

// Utility to format data types
const formatDataType = (type) => {
  if (type.includes("date32[day]")) return "Date";
  if (type.includes("int")) return "Integer";
  if (type.includes("string")) return "String";
  return type;
};

// Utility to compute statistics for a column
const computeColumnStats = (data, column) => {
  const values = data.map((row) => row[column]);
  const isNumeric = values.every((val) => !isNaN(Number(val)) && val !== null);

  if (isNumeric) {
    const numericValues = values.map((val) => Number(val)).filter((val) => !isNaN(val));
    if (numericValues.length === 0) {
      return { type: "numeric", min: null, max: null, mean: null, std: null };
    }
    const min = Math.min(...numericValues);
    const max = Math.max(...numericValues);
    const mean = numericValues.reduce((sum, val) => sum + val, 0) / numericValues.length;
    const variance = numericValues.reduce((sum, val) => sum + Math.pow(val - mean, 2), 0) / numericValues.length;
    const std = Math.sqrt(variance);
    return { type: "numeric", min, max, mean, std };
  } else {
    const frequency = {};
    values.forEach((val) => {
      if (val !== null) {
        frequency[val] = (frequency[val] || 0) + 1;
      }
    });
    const sortedFreq = Object.entries(frequency)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 5);
    return { type: "categorical", topValues: sortedFreq };
  }
};

// Utility to generate histogram data for numeric columns
const generateHistogramData = (data, column) => {
  const values = data.map((row) => Number(row[column])).filter((val) => !isNaN(val));
  if (values.length === 0) return null;

  const min = Math.min(...values);
  const max = Math.max(...values);
  const numBins = 10;
  const binWidth = (max - min) / numBins;
  const bins = Array(numBins).fill(0);

  values.forEach((val) => {
    const binIndex = Math.min(Math.floor((val - min) / binWidth), numBins - 1);
    bins[binIndex]++;
  });

  const labels = Array.from({ length: numBins }, (_, i) => {
    const start = min + i * binWidth;
    const end = start + binWidth;
    return `${start.toFixed(2)} - ${end.toFixed(2)}`;
  });

  return {
    labels,
    datasets: [
      {
        label: `Distribution of ${column}`,
        data: bins,
        backgroundColor: "rgba(74, 144, 226, 0.6)", // Glacier Blue with opacity
        borderColor: "#4A90E2", // Glacier Blue
        borderWidth: 1,
      },
    ],
  };
};

// Utility to generate bar chart data for categorical columns
const generateCategoricalBarData = (topValues, column) => {
  const labels = topValues.map(([value]) => value);
  const data = topValues.map(([, count]) => count);

  return {
    labels,
    datasets: [
      {
        label: `Top Values in ${column}`,
        data,
        backgroundColor: "rgba(59, 105, 120, 0.6)", // Slate Teal with opacity
        borderColor: "#3B6978", // Slate Teal
        borderWidth: 1,
      },
    ],
  };
};

const MetadataViewer = () => {
  const [objectStorePath, setObjectStorePath] = useState("s3://test-bucket");
  const [metadata, setMetadata] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [searchQuery, setSearchQuery] = useState("");
  const [expandedItem, setExpandedItem] = useState(null);
  const [darkMode, setDarkMode] = useState(false);
  const [dataMap, setDataMap] = useState({});
  const [loadingDataMap, setLoadingDataMap] = useState({});
  const [currentPage, setCurrentPage] = useState(1);
  const [selectedChartColumn, setSelectedChartColumn] = useState({});
  const [pinnedColumns, setPinnedColumns] = useState({});
  const [hiddenColumns, setHiddenColumns] = useState({});

  const itemsPerPage = 10;

  // Debug darkMode state
  useEffect(() => {
    console.log("Dark Mode State:", darkMode);
    if (!darkMode) {
      document.documentElement.classList.remove("dark");
    } else {
      document.documentElement.classList.add("dark");
    }
  }, [darkMode]);

  const parsePath = (path) => {
    const match = path.match(/^(s3|azure|minio):\/\/([^/]+)(?:\/(.+))?$/);
    if (!match) return { type: null, bucket: null, prefix: "" };
    return { type: match[1], bucket: match[2], prefix: match[3] || "" };
  };

  const fetchMetadata = useCallback(async () => {
    setLoading(true);
    setError(null);
    setMetadata([]);
    setDataMap({});
    setExpandedItem(null);
    setPinnedColumns({});
    setHiddenColumns({});

    const { type, bucket, prefix } = parsePath(objectStorePath);
    if (!bucket) {
      setError("Invalid path. Use: s3://bucket-name/[prefix]");
      setLoading(false);
      return;
    }

    try {
      const response = await axios.get("http://127.0.0.1:5000/metadata", {
        params: { bucket, prefix },
      });
      console.log("Backend response:", response.data);
      const files = response.data.files || [];
      console.log("Setting metadata to:", files);
      setMetadata(files);
      setCurrentPage(1); // Reset to page 1 on new fetch
    } catch (err) {
      console.error("Fetch error:", err);
      setError(err.response?.data?.error || "Failed to fetch metadata");
      setMetadata([]);
    } finally {
      setLoading(false);
    }
  }, [objectStorePath]);

  const fetchData = async (item) => {
    setLoadingDataMap((prev) => ({ ...prev, [item.file]: true }));
    try {
      const response = await axios.get("http://127.0.0.1:5000/data", {
        params: { file: item.file, bucket: parsePath(objectStorePath).bucket },
      });
      setDataMap((prev) => ({ ...prev, [item.file]: response.data }));
      const columns = Object.keys(response.data.data[0] || {});
      setHiddenColumns((prev) => ({
        ...prev,
        [item.file]: columns.reduce((acc, col) => ({ ...acc, [col]: false }), {}),
      }));
    } catch (err) {
      setDataMap((prev) => ({ ...prev, [item.file]: { error: "Failed to load data" } }));
    } finally {
      setLoadingDataMap((prev) => ({ ...prev, [item.file]: false }));
    }
  };

  const handleSubmit = (e) => {
    e.preventDefault();
    fetchMetadata();
  };

  const toggleExpand = (file) => {
    setExpandedItem(expandedItem === file ? null : file);
  };

  const handleSearch = debounce((value) => {
    setSearchQuery(value);
    setCurrentPage(1); // Reset to page 1 on search
  }, 300);

  const togglePinColumn = (file, column) => {
    setPinnedColumns((prev) => ({
      ...prev,
      [file]: {
        ...prev[file],
        [column]: !prev[file]?.[column],
      },
    }));
  };

  const toggleHideColumn = (file, column) => {
    setHiddenColumns((prev) => ({
      ...prev,
      [file]: {
        ...prev[file],
        [column]: !prev[file]?.[column],
      },
    }));
  };

  const filteredMetadata = useMemo(() => {
    if (!Array.isArray(metadata)) return [];
    return metadata
      .map((item) => {
        const parts = item.file.split(".");
        const filename = parts.slice(0, -1).join(".") || item.file;
        const extension = parts.length > 1 ? parts[parts.length - 1] : (item.details && item.details.format ? item.details.format : "");
        return { ...item, filename, extension };
      })
      .filter((item) => item.filename.toLowerCase().includes(searchQuery.toLowerCase()));
  }, [metadata, searchQuery]);

  const paginatedMetadata = useMemo(() => {
    return filteredMetadata.slice((currentPage - 1) * itemsPerPage, currentPage * itemsPerPage);
  }, [filteredMetadata, currentPage]);

  useEffect(() => {
    // Ensure currentPage is valid after filtering
    const maxPage = Math.ceil(filteredMetadata.length / itemsPerPage);
    if (currentPage > maxPage && maxPage > 0) {
      setCurrentPage(maxPage);
    }
  }, [filteredMetadata, currentPage]);

  console.log("Metadata state:", metadata);
  console.log("Filtered metadata:", filteredMetadata);
  console.log("Paginated metadata:", paginatedMetadata);

  const LoadingSpinner = () => (
    <div className="flex justify-center items-center h-32">
      <div className="animate-spin rounded-full h-12 w-12 border-t-2 border-b-2 border-glacier-blue dark:border-frost-white"></div>
    </div>
  );

  return (
    <ErrorBoundary>
      <div className="min-h-screen p-6 transition-all">
        <form onSubmit={handleSubmit} className="mb-6">
          <div className="flex flex-col sm:flex-row gap-4 items-center">
            <input
              type="text"
              placeholder="Enter path (e.g., s3://bucket/prefix)"
              value={objectStorePath}
              onChange={(e) => setObjectStorePath(e.target.value)}
              className="px-4 py-2 border rounded-lg w-full sm:w-2/3 shadow-md focus:outline-none focus:ring-2 focus:ring-glacier-blue bg-frost-white/80 dark:bg-midnight-blue/80 backdrop-blur-md border-mist-gray/20 dark:border-frost-white/20 text-midnight-blue dark:text-frost-white placeholder-mist-gray dark:placeholder-frost-white transition-all duration-300"
            />
            <button
              type="submit"
              className="px-4 py-2 bg-glacier-blue text-frost-white rounded-md hover:scale-105 hover:shadow-lg focus:outline-none focus:ring-2 focus:ring-glacier-blue transition-all duration-300 active:scale-95"
              disabled={loading}
            >
              Load Metadata
            </button>
            {/* Dark mode toggle removed from here since it's now in Home.js */}
          </div>
        </form>

        <input
          type="text"
          placeholder="Search tables/files..."
          onChange={(e) => handleSearch(e.target.value)}
          className="mb-6 px-4 py-2 border rounded-lg w-full shadow-md focus:outline-none focus:ring-2 focus:ring-glacier-blue bg-frost-white/80 dark:bg-midnight-blue/80 backdrop-blur-md border-mist-gray/20 dark:border-frost-white/20 text-midnight-blue dark:text-frost-white placeholder-mist-gray dark:placeholder-frost-white transition-all duration-300"
        />

        {loading ? (
          <LoadingSpinner />
        ) : error ? (
          <p className="text-red-500 font-semibold text-center">{error}</p>
        ) : (
          <>
            <div className="overflow-x-auto w-full max-w-6xl mx-auto">
              <table className="w-full shadow-lg rounded-lg overflow-hidden bg-frost-white/80 dark:bg-midnight-blue/80 backdrop-blur-md border border-mist-gray/20 dark:border-frost-white/20">
                <thead className="bg-mist-gray dark:bg-slate-teal text-midnight-blue dark:text-frost-white">
                  <tr>
                    <th className="px-6 py-3 text-left font-semibold">Table/File Name</th>
                    <th className="px-6 py-3 text-left font-semibold">Format</th>
                    <th className="px-6 py-3 text-left font-semibold">Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {paginatedMetadata.length > 0 ? (
                    paginatedMetadata.map((item) => (
                      <React.Fragment key={item.file}>
                        <tr className="hover:bg-glacier-blue/10 dark:hover:bg-frost-white/10 transition-all duration-200">
                          <td className="px-6 py-4 font-mono flex items-center space-x-2 text-midnight-blue dark:text-frost-white">
                            {getFileIcon(item.extension)}
                            <span>{item.file}</span>
                          </td>
                          <td className="px-6 py-4 text-mist-gray dark:text-glacier-blue">
                            {item.details && item.details.format ? item.details.format : "N/A"}
                          </td>
                          <td className="px-6 py-4">
                            <div className="flex space-x-2">
                              <button
                                className="px-3 py-2 bg-glacier-blue text-frost-white rounded-md hover:scale-105 hover:shadow-lg focus:outline-none focus:ring-2 focus:ring-glacier-blue transition-all duration-300 active:scale-95"
                                onClick={() => toggleExpand(item.file)}
                              >
                                {expandedItem === item.file ? "Hide Details" : "View Details"}
                              </button>
                              {item.extension === "parquet" && (
                                <button
                                  className="px-3 py-2 bg-glacier-blue text-frost-white rounded-md hover:scale-105 hover:shadow-lg focus:outline-none focus:ring-2 focus:ring-glacier-blue transition-all duration-300 active:scale-95"
                                  onClick={() => {
                                    if (dataMap[item.file]) {
                                      setDataMap((prev) => {
                                        const newMap = { ...prev };
                                        delete newMap[item.file];
                                        return newMap;
                                      });
                                    } else {
                                      fetchData(item);
                                    }
                                  }}
                                >
                                  {dataMap[item.file] ? "Hide Data" : "View Data"}
                                </button>
                              )}
                            </div>
                          </td>
                        </tr>

                        {expandedItem === item.file && (
                          <tr>
                            <td colSpan={3} className="p-6">
                              <div className="p-4 rounded-lg shadow-md border-l-4 border-glacier-blue dark:border-frost-white bg-frost-white/80 dark:bg-midnight-blue/80 backdrop-blur-md">
                                {item.details && item.details.error && (
                                  <p className="text-red-500 mb-4">Error: {item.details.error}</p>
                                )}
                                <h3 className="text-lg font-semibold mb-2 text-slate-teal dark:text-glacier-blue">Schema</h3>
                                <div className="grid grid-cols-3 gap-4 text-sm font-mono">
                                  {item.details && Array.isArray(item.details.columns) && item.details.columns.length > 0 ? (
                                    item.details.columns.map((col, i) => (
                                      <div key={i} className="flex flex-col p-2 border-b border-mist-gray/20 dark:border-frost-white/20">
                                        <span className="font-semibold text-glacier-blue">{col.name}</span>
                                        <span className="text-midnight-blue dark:text-frost-white">{formatDataType(col.type)}</span>
                                        <span className="text-mist-gray dark:text-glacier-blue">{col.nullable ? "Nullable" : "Not Nullable"}</span>
                                      </div>
                                    ))
                                  ) : (
                                    <p className="text-mist-gray dark:text-frost-white">No schema available</p>
                                  )}
                                </div>
                                <h3 className="text-lg font-semibold mt-4 text-slate-teal dark:text-glacier-blue">Partition Details</h3>
                                {item.details && Array.isArray(item.details.partition_keys) && item.details.partition_keys.length > 0 ? (
                                  <ul className="list-disc pl-5 text-midnight-blue dark:text-frost-white">
                                    {item.details.partition_keys.map((key, i) => (
                                      <li key={i}>{key}</li>
                                    ))}
                                  </ul>
                                ) : (
                                  <p className="text-mist-gray dark:text-frost-white">No partitions</p>
                                )}
                                <h3 className="text-lg font-semibold mt-4 text-slate-teal dark:text-glacier-blue">Snapshots/Versions</h3>
                                {item.details && Array.isArray(item.details.snapshots) && item.details.snapshots.length > 0 ? (
                                  <ul className="list-disc pl-5 text-midnight-blue dark:text-frost-white">
                                    {item.details.snapshots.map((snap, i) => (
                                      <li key={i}>
                                        ID: {snap.id || snap.version}, Timestamp: {snap.timestamp}
                                      </li>
                                    ))}
                                  </ul>
                                ) : (
                                  <p className="text-mist-gray dark:text-frost-white">No snapshots</p>
                                )}
                                <h3 className="text-lg font-semibold mt-4 text-slate-teal dark:text-glacier-blue">Key Metrics</h3>
                                <p className="text-midnight-blue dark:text-frost-white">
                                  File Size: {item.details && item.details.file_size ? formatFileSize(item.details.file_size) : "N/A"}
                                </p>
                                <p className="text-midnight-blue dark:text-frost-white">
                                  Row Count: {item.details && item.details.num_rows ? formatNumber(item.details.num_rows) : "N/A"}
                                </p>
                              </div>
                            </td>
                          </tr>
                        )}

                        {dataMap[item.file] && (
                          <tr>
                            <td colSpan={3} className="p-6">
                              <div className="p-4 rounded-lg shadow-md border-l-4 border-glacier-blue dark:border-frost-white bg-frost-white/80 dark:bg-midnight-blue/80 backdrop-blur-md">
                                <div className="flex justify-between items-center mb-2">
                                  <h3 className="text-lg font-semibold text-slate-teal dark:text-glacier-blue">
                                    Sample Data: {dataMap[item.file].file}
                                  </h3>
                                  <button
                                    className="text-sm text-glacier-blue hover:underline"
                                    onClick={() =>
                                      setDataMap((prev) => {
                                        const newMap = { ...prev };
                                        delete newMap[item.file];
                                        return newMap;
                                      })
                                    }
                                  >
                                    Close
                                  </button>
                                </div>
                                {loadingDataMap[item.file] ? (
                                  <LoadingSpinner />
                                ) : dataMap[item.file].error ? (
                                  <p className="text-red-500">{dataMap[item.file].error}</p>
                                ) : (
                                  <>
                                    <div className="mb-4">
                                      <h4 className="text-md font-semibold mb-2 text-slate-teal dark:text-glacier-blue">Manage Columns</h4>
                                      <div className="flex flex-wrap gap-2">
                                        {Object.keys(dataMap[item.file].data[0] || {}).map((key) => (
                                          <div key={key} className="flex items-center space-x-2">
                                            <input
                                              type="checkbox"
                                              checked={!hiddenColumns[item.file]?.[key]}
                                              onChange={() => toggleHideColumn(item.file, key)}
                                            />
                                            <label className="text-sm text-midnight-blue dark:text-frost-white">{key}</label>
                                            <button
                                              onClick={() => togglePinColumn(item.file, key)}
                                              className={`text-sm px-2 py-1 rounded ${
                                                pinnedColumns[item.file]?.[key]
                                                  ? "bg-glacier-blue text-frost-white"
                                                  : "bg-mist-gray text-midnight-blue dark:bg-slate-teal dark:text-frost-white"
                                              } hover:scale-105 transition-all duration-300`}
                                            >
                                              {pinnedColumns[item.file]?.[key] ? "Unpin" : "Pin"}
                                            </button>
                                          </div>
                                        ))}
                                      </div>
                                    </div>
                                    <div className="overflow-x-auto max-h-96 mb-6 w-full max-w-6xl mx-auto">
                                      <div className="inline-block">
                                        <table className="border-collapse border border-mist-gray/20 dark:border-frost-white/20 bg-frost-white/80 dark:bg-midnight-blue/80 backdrop-blur-md">
                                          <thead>
                                            <tr className="bg-mist-gray dark:bg-slate-teal sticky top-0 z-10">
                                              {Object.keys(dataMap[item.file].data[0] || {})
                                                .filter((key) => !hiddenColumns[item.file]?.[key])
                                                .map((key, idx) => (
                                                  <th
                                                    key={key}
                                                    className={`border p-2 whitespace-nowrap text-midnight-blue dark:text-frost-white ${
                                                      pinnedColumns[item.file]?.[key] ? "sticky z-20 bg-inherit" : ""
                                                    }`}
                                                    style={{
                                                      minWidth: "150px",
                                                      left: pinnedColumns[item.file]?.[key]
                                                        ? `${
                                                            Object.keys(dataMap[item.file].data[0] || {})
                                                              .filter((k) => !hiddenColumns[item.file]?.[k] && pinnedColumns[item.file]?.[k])
                                                              .slice(
                                                                0,
                                                                Object.keys(dataMap[item.file].data[0] || {})
                                                                  .filter((k) => !hiddenColumns[item.file]?.[k] && pinnedColumns[item.file]?.[k])
                                                                  .indexOf(key)
                                                              )
                                                              .reduce((acc, k) => acc + 150, 0)
                                                          }px`
                                                        : undefined,
                                                    }}
                                                  >
                                                    {key}
                                                  </th>
                                                ))}
                                            </tr>
                                          </thead>
                                          <tbody>
                                            {dataMap[item.file].data.map((row, idx) => (
                                              <tr key={idx} className="hover:bg-glacier-blue/10 dark:hover:bg-frost-white/10 transition-all duration-200">
                                                {Object.keys(row)
                                                  .filter((key) => !hiddenColumns[item.file]?.[key])
                                                  .map((key, i) => (
                                                    <td
                                                      key={i}
                                                      className={`border p-2 whitespace-nowrap text-midnight-blue dark:text-frost-white ${
                                                        pinnedColumns[item.file]?.[key] ? "sticky z-10 bg-inherit" : ""
                                                      }`}
                                                      style={{
                                                        minWidth: "150px",
                                                        left: pinnedColumns[item.file]?.[key]
                                                          ? `${
                                                              Object.keys(dataMap[item.file].data[0] || {})
                                                                .filter((k) => !hiddenColumns[item.file]?.[k] && pinnedColumns[item.file]?.[k])
                                                                .slice(
                                                                  0,
                                                                  Object.keys(dataMap[item.file].data[0] || {})
                                                                    .filter((k) => !hiddenColumns[item.file]?.[k] && pinnedColumns[item.file]?.[k])
                                                                    .indexOf(key)
                                                                )
                                                                .reduce((acc, k) => acc + 150, 0)
                                                            }px`
                                                          : undefined,
                                                      }}
                                                    >
                                                      {row[key]}
                                                    </td>
                                                  ))}
                                              </tr>
                                            ))}
                                          </tbody>
                                        </table>
                                      </div>
                                    </div>
                                    <div className="mt-6 w-full">
                                      <h4 className="text-lg font-semibold mb-2 text-slate-teal dark:text-glacier-blue">Column Statistics</h4>
                                      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
                                        {Object.keys(dataMap[item.file].data[0] || {}).length > 0 ? (
                                          Object.keys(dataMap[item.file].data[0]).map((col, idx) => {
                                            const stats = computeColumnStats(dataMap[item.file].data, col);
                                            return (
                                              <div
                                                key={idx}
                                                className="p-4 border rounded-lg shadow-sm bg-frost-white/80 dark:bg-midnight-blue/80 backdrop-blur-md border-mist-gray/20 dark:border-frost-white/20 text-midnight-blue dark:text-frost-white"
                                              >
                                                <h5 className="text-md font-medium mb-2 text-glacier-blue">{col}</h5>
                                                {stats.type === "numeric" ? (
                                                  <div className="text-sm">
                                                    <p>Min: {stats.min !== null ? stats.min.toFixed(2) : "N/A"}</p>
                                                    <p>Max: {stats.max !== null ? stats.max.toFixed(2) : "N/A"}</p>
                                                    <p>Mean: {stats.mean !== null ? stats.mean.toFixed(2) : "N/A"}</p>
                                                    <p>Std Dev: {stats.std !== null ? stats.std.toFixed(2) : "N/A"}</p>
                                                  </div>
                                                ) : (
                                                  <div className="text-sm">
                                                    <p>Top 5 Values:</p>
                                                    <ul className="list-disc pl-5">
                                                      {stats.topValues.map(([value, count], i) => (
                                                        <li key={i}>
                                                          {value}: {count}
                                                        </li>
                                                      ))}
                                                    </ul>
                                                  </div>
                                                )}
                                              </div>
                                            );
                                          })
                                        ) : (
                                          <p className="col-span-full text-center text-mist-gray dark:text-frost-white">
                                            No column statistics available.
                                          </p>
                                        )}
                                      </div>
                                      <h3 className="text-lg font-semibold mb-2 mt-6 text-slate-teal dark:text-glacier-blue">Data Distribution</h3>
                                      <select
                                        className="mb-4 p-2 border rounded-md bg-frost-white/80 dark:bg-midnight-blue/80 backdrop-blur-md border-mist-gray/20 dark:border-frost-white/20 text-midnight-blue dark:text-frost-white focus:outline-none focus:ring-2 focus:ring-glacier-blue transition-all duration-300"
                                        value={selectedChartColumn[item.file] || ""}
                                        onChange={(e) =>
                                          setSelectedChartColumn((prev) => ({ ...prev, [item.file]: e.target.value }))
                                        }
                                      >
                                        <option value="">Select a column to visualize</option>
                                        {Object.keys(dataMap[item.file].data[0] || {}).map((col) => (
                                          <option key={col} value={col}>
                                            {col}
                                          </option>
                                        ))}
                                      </select>
                                      {selectedChartColumn[item.file] && (
                                        (() => {
                                          const stats = computeColumnStats(dataMap[item.file].data, selectedChartColumn[item.file]);
                                          if (stats.type === "numeric") {
                                            const histogramData = generateHistogramData(dataMap[item.file].data, selectedChartColumn[item.file]);
                                            if (!histogramData)
                                              return <p className="text-mist-gray dark:text-frost-white">No numeric data available for visualization</p>;
                                            return (
                                              <div className="w-full max-w-md mx-auto">
                                                <h5 className="text-md font-medium mb-2 text-glacier-blue">Histogram</h5>
                                                <div style={{ height: "400px", width: "100%", backgroundColor: darkMode ? "#1A1D2E" : "#F5F7FA" }}>
                                                  <Bar
                                                    data={histogramData}
                                                    options={{
                                                      responsive: true,
                                                      maintainAspectRatio: false,
                                                      plugins: {
                                                        legend: { position: "top", labels: { color: darkMode ? "#F5F7FA" : "#1A1D2E" } },
                                                        title: {
                                                          display: true,
                                                          text: `${selectedChartColumn[item.file]} Distribution`,
                                                          color: darkMode ? "#F5F7FA" : "#1A1D2E",
                                                        },
                                                        tooltip: {
                                                          backgroundColor: darkMode ? "#3B6978" : "#F5F7FA",
                                                          titleColor: darkMode ? "#F5F7FA" : "#1A1D2E",
                                                          bodyColor: darkMode ? "#F5F7FA" : "#1A1D2E",
                                                        },
                                                      },
                                                      scales: {
                                                        x: {
                                                          ticks: { color: darkMode ? "#F5F7FA" : "#1A1D2E" },
                                                          grid: { color: darkMode ? "#3B6978" : "#A3BFFA" },
                                                        },
                                                        y: {
                                                          beginAtZero: true,
                                                          ticks: { color: darkMode ? "#F5F7FA" : "#1A1D2E" },
                                                          grid: { color: darkMode ? "#3B6978" : "#A3BFFA" },
                                                        },
                                                      },
                                                    }}
                                                  />
                                                </div>
                                              </div>
                                            );
                                          } else {
                                            const barData = generateCategoricalBarData(stats.topValues, selectedChartColumn[item.file]);
                                            return (
                                              <div className="w-full max-w-md mx-auto">
                                                <h5 className="text-md font-medium mb-2 text-glacier-blue">Top Values</h5>
                                                <div style={{ height: "400px", width: "100%", backgroundColor: darkMode ? "#1A1D2E" : "#F5F7FA" }}>
                                                  <Bar
                                                    data={barData}
                                                    options={{
                                                      responsive: true,
                                                      maintainAspectRatio: false,
                                                      plugins: {
                                                        legend: { position: "top", labels: { color: darkMode ? "#F5F7FA" : "#1A1D2E" } },
                                                        title: {
                                                          display: true,
                                                          text: `Top Values in ${selectedChartColumn[item.file]}`,
                                                          color: darkMode ? "#F5F7FA" : "#1A1D2E",
                                                        },
                                                        tooltip: {
                                                          backgroundColor: darkMode ? "#3B6978" : "#F5F7FA",
                                                          titleColor: darkMode ? "#F5F7FA" : "#1A1D2E",
                                                          bodyColor: darkMode ? "#F5F7FA" : "#1A1D2E",
                                                        },
                                                      },
                                                      scales: {
                                                        x: {
                                                          ticks: { color: darkMode ? "#F5F7FA" : "#1A1D2E" },
                                                          grid: { color: darkMode ? "#3B6978" : "#A3BFFA" },
                                                        },
                                                        y: {
                                                          beginAtZero: true,
                                                          ticks: { color: darkMode ? "#F5F7FA" : "#1A1D2E" },
                                                          grid: { color: darkMode ? "#3B6978" : "#A3BFFA" },
                                                        },
                                                      },
                                                    }}
                                                  />
                                                </div>
                                              </div>
                                            );
                                          }
                                        })()
                                      )}
                                    </div>
                                  </>
                                )}
                              </div>
                            </td>
                          </tr>
                        )}
                      </React.Fragment>
                    ))
                  ) : (
                    <tr>
                      <td colSpan={3} className="px-6 py-4 text-center text-mist-gray dark:text-frost-white">
                        No metadata available
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>

            {filteredMetadata.length > itemsPerPage && (
              <div className="flex justify-center items-center mt-6 space-x-4">
                <button
                  onClick={() => setCurrentPage((p) => Math.max(1, p - 1))}
                  disabled={currentPage === 1}
                  className="px-4 py-2 bg-glacier-blue text-frost-white rounded-md disabled:bg-mist-gray disabled:cursor-not-allowed hover:scale-105 hover:shadow-lg focus:outline-none focus:ring-2 focus:ring-glacier-blue transition-all duration-300 active:scale-95"
                >
                  Previous
                </button>
                <span className="text-sm text-midnight-blue dark:text-frost-white">
                  Page {currentPage} of {Math.ceil(filteredMetadata.length / itemsPerPage)}
                </span>
                <button
                  onClick={() => setCurrentPage((p) => p + 1)}
                  disabled={currentPage * itemsPerPage >= filteredMetadata.length}
                  className="px-4 py-2 bg-glacier-blue text-frost-white rounded-md disabled:bg-mist-gray disabled:cursor-not-allowed hover:scale-105 hover:shadow-lg focus:outline-none focus:ring-2 focus:ring-glacier-blue transition-all duration-300 active:scale-95"
                >
                  Next
                </button>
              </div>
            )}
          </>
        )}
      </div>
    </ErrorBoundary>
  );
};

export default MetadataViewer;