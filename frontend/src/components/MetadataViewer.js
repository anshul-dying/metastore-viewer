// import React, { useState, useEffect, Component } from "react";
// import axios from "axios";
// import { FaFileAlt, FaFileExcel, FaFilePdf, FaFileImage, FaFileCode } from "react-icons/fa";
// import { Bar, Pie } from "react-chartjs-2";
// import {
//     Chart as ChartJS,
//     CategoryScale,
//     LinearScale,
//     BarElement,
//     ArcElement,
//     Title,
//     Tooltip,
//     Legend,
// } from "chart.js";

// // Register ChartJS components
// ChartJS.register(
//     CategoryScale,
//     LinearScale,
//     BarElement,
//     ArcElement,
//     Title,
//     Tooltip,
//     Legend
// );

// // Error Boundary (unchanged)
// class ErrorBoundary extends Component {
//     state = { hasError: false };
//     static getDerivedStateFromError() {
//         return { hasError: true };
//     }
//     render() {
//         if (this.state.hasError) {
//             return <p className="text-red-500 text-center">Something went wrong. Please try again.</p>;
//         }
//         return this.props.children;
//     }
// }

// // Utility functions (unchanged)
// const getFileIcon = (extension) => {
//     const ext = extension.toLowerCase();
//     if (["xls", "xlsx", "csv"].includes(ext)) return <FaFileExcel className="text-green-500" />;
//     if (["pdf"].includes(ext)) return <FaFilePdf className="text-red-500" />;
//     if (["jpg", "jpeg", "png", "gif"].includes(ext)) return <FaFileImage className="text-yellow-500" />;
//     if (["js", "jsx", "ts", "tsx", "html", "css", "json"].includes(ext)) return <FaFileCode className="text-blue-500" />;
//     return <FaFileAlt className="text-gray-500" />;
// };

// const debounce = (func, delay) => {
//     let timeoutId;
//     return (...args) => {
//         clearTimeout(timeoutId);
//         timeoutId = setTimeout(() => func(...args), delay);
//     };
// };

// const MetadataViewer = () => {
//     const [metadata, setMetadata] = useState([]);
//     const [loading, setLoading] = useState(true);
//     const [error, setError] = useState(null);
//     const [searchQuery, setSearchQuery] = useState("");
//     const [expandedFile, setExpandedFile] = useState(null);
//     const [darkMode, setDarkMode] = useState(false);
//     const [fileDataMap, setFileDataMap] = useState({});
//     const [loadingDataMap, setLoadingDataMap] = useState({});
//     const [currentPage, setCurrentPage] = useState(1);
//     const [selectedChartColumn, setSelectedChartColumn] = useState({});
//     const itemsPerPage = 10;

//     const fetchFileData = async (file) => {
//         setLoadingDataMap(prev => ({ ...prev, [file]: true }));
//         try {
//             const response = await axios.get(`http://127.0.0.1:5000/data?file=${file}`);
//             setFileDataMap(prev => ({ ...prev, [file]: response.data }));
//         } catch (err) {
//             setFileDataMap(prev => ({ ...prev, [file]: { error: "Failed to load data" } }));
//         } finally {
//             setLoadingDataMap(prev => ({ ...prev, [file]: false }));
//         }
//     };

//     useEffect(() => {
//         const controller = new AbortController();
//         const fetchMetadata = async () => {
//             try {
//                 const response = await axios.get("http://127.0.0.1:5000/metadata?bucket=test-bucket", {
//                     signal: controller.signal
//                 });
//                 setMetadata(response.data.files || []);
//             } catch (err) {
//                 if (!axios.isCancel(err)) {
//                     setError("Failed to fetch metadata");
//                 }
//             } finally {
//                 setLoading(false);
//             }
//         };
//         fetchMetadata();
//         return () => controller.abort();
//     }, []);

//     const toggleExpand = (file) => {
//         setExpandedFile(expandedFile === file ? null : file);
//     };

//     const handleSearch = debounce((value) => {
//         setSearchQuery(value);
//         setCurrentPage(1);
//     }, 300);

//     const filteredMetadata = metadata.map((file) => {
//         const parts = file.file.split(".");
//         const filename = parts.slice(0, -1).join(".") || file.file;
//         const extension = parts.length > 1 ? parts[parts.length - 1] : "";
//         return { ...file, filename, extension };
//     }).filter((file) =>
//         file.filename.toLowerCase().includes(searchQuery.toLowerCase())
//     );

//     const paginatedMetadata = filteredMetadata.slice(
//         (currentPage - 1) * itemsPerPage,
//         currentPage * itemsPerPage
//     );

//     const LoadingSpinner = () => (
//         <div className="flex justify-center items-center h-32">
//             <div className="animate-spin rounded-full h-12 w-12 border-t-2 border-b-2 border-blue-500"></div>
//         </div>
//     );

//     // Chart generation function
//     const generateChartData = (fileData, column) => {
//         if (!fileData?.data || !column) return null;

//         const numericData = fileData.data
//             .map(row => Number(row[column]))
//             .filter(val => !isNaN(val));

//         if (numericData.length === 0) return null;

//         // For simplicity, we'll create a frequency distribution for bar chart
//         const frequency = {};
//         numericData.forEach(val => {
//             frequency[val] = (frequency[val] || 0) + 1;
//         });

//         const labels = Object.keys(frequency);
//         const values = Object.values(frequency);

//         return {
//             bar: {
//                 labels,
//                 datasets: [{
//                     label: `Distribution of ${column}`,
//                     data: values,
//                     backgroundColor: 'rgba(75, 192, 192, 0.6)',
//                     borderColor: 'rgba(75, 192, 192, 1)',
//                     borderWidth: 1,
//                 }]
//             },
//             pie: {
//                 labels,
//                 datasets: [{
//                     data: values,
//                     backgroundColor: [
//                         'rgba(255, 99, 132, 0.6)',
//                         'rgba(54, 162, 235, 0.6)',
//                         'rgba(255, 206, 86, 0.6)',
//                         'rgba(75, 192, 192, 0.6)',
//                         'rgba(153, 102, 255, 0.6)',
//                     ],
//                 }]
//             }
//         };
//     };

//     return (
//         <ErrorBoundary>
//             <div className={`min-h-screen p-6 transition-all ${darkMode ? "bg-gray-900 text-white" : "bg-gray-100 text-black"}`}>
//                 {/* Header Section (unchanged) */}
//                 <div className="flex flex-col sm:flex-row justify-between items-center mb-6 gap-4">
//                     <input
//                         type="text"
//                         placeholder="Search files..."
//                         onChange={(e) => handleSearch(e.target.value)}
//                         className={`px-4 py-2 border rounded-lg w-full sm:w-1/3 shadow-md focus:outline-none focus:ring-2 focus:ring-blue-400 text-black`}
//                     />
//                     <div className="flex items-center space-x-3">
//                         <span className="text-sm font-medium">DARK</span>
//                         <label className="relative inline-flex items-center cursor-pointer">
//                             <input
//                                 type="checkbox"
//                                 className="sr-only peer"
//                                 checked={darkMode}
//                                 onChange={() => setDarkMode(!darkMode)}
//                             />
//                             <div className="w-14 h-7 bg-gray-300 peer-focus:ring-4 peer-focus:ring-blue-300 rounded-full peer dark:bg-gray-700 peer-checked:after:translate-x-7 peer-checked:after:border-white after:content-[''] after:absolute after:top-0.5 after:left-0.5 after:bg-white after:border after:rounded-full after:h-6 after:w-6 after:transition-all peer-checked:bg-blue-600"></div>
//                         </label>
//                         <span className="text-sm font-medium">LIGHT</span>
//                     </div>
//                 </div>

//                 {/* Loading & Error Handling */}
//                 {loading ? (
//                     <LoadingSpinner />
//                 ) : error ? (
//                     <p className="text-red-500 font-semibold text-center">{error}</p>
//                 ) : (
//                     <>
//                         <div className="overflow-x-auto">
//                             <table className={`w-full shadow-lg rounded-lg overflow-hidden ${darkMode ? "bg-gray-800" : "bg-white"}`}>
//                                 <thead className={darkMode ? "bg-gray-700" : "bg-gray-200"}>
//                                     <tr>
//                                         <th className="px-6 py-3 text-left font-semibold">File Name</th>
//                                         <th className="px-6 py-3 text-left font-semibold">Actions</th>
//                                     </tr>
//                                 </thead>
//                                 <tbody>
//                                     {paginatedMetadata.map((file, index) => (
//                                         <React.Fragment key={index}>
//                                             <tr className={darkMode ? "hover:bg-gray-600" : "hover:bg-gray-100"}>
//                                                 <td className="px-6 py-4 font-mono flex items-center space-x-2">
//                                                     {getFileIcon(file.extension)}
//                                                     <span>{file.filename}</span>
//                                                     {file.extension && <span className="text-gray-500">.{file.extension}</span>}
//                                                 </td>
//                                                 <td className="px-6 py-4">
//                                                     <div className="flex space-x-2">
//                                                         <button
//                                                             className="px-3 py-2 bg-blue-500 text-white rounded-md hover:bg-blue-600 focus:outline-none focus:ring-2 focus:ring-blue-300"
//                                                             onClick={() => toggleExpand(file.file)}
//                                                         >
//                                                             {expandedFile === file.file ? "Hide Details" : "View Details"}
//                                                         </button>
//                                                         <button
//                                                             className="px-3 py-2 bg-green-500 text-white rounded-md hover:bg-green-600 focus:outline-none focus:ring-2 focus:ring-green-300"
//                                                             onClick={() => {
//                                                                 if (fileDataMap[file.file]) {
//                                                                     setFileDataMap(prev => {
//                                                                         const newMap = { ...prev };
//                                                                         delete newMap[file.file];
//                                                                         return newMap;
//                                                                     });
//                                                                 } else {
//                                                                     fetchFileData(file.file);
//                                                                 }
//                                                             }}
//                                                         >
//                                                             {fileDataMap[file.file] ? "Hide Data" : "View Data"}
//                                                         </button>
//                                                     </div>
//                                                 </td>
//                                             </tr>

//                                             {/* File Details (unchanged) */}
//                                             {expandedFile === file.file && (
//                                                 <tr>
//                                                     <td colSpan={2} className="p-6">
//                                                         <div className={`p-4 rounded-lg shadow-md border-l-4 border-blue-500 ${darkMode ? "bg-gray-700" : "bg-gray-50"}`}>
//                                                             <p className="text-lg font-semibold mb-2">Columns:</p>
//                                                             <div className="grid grid-cols-2 gap-4 text-sm font-mono">
//                                                                 {file.details.columns.map((col, i) => {
//                                                                     const [name, type] = col.replace('pyarrow.Field<', '').replace('>', '').split(': ');
//                                                                     return (
//                                                                         <div key={i} className="flex justify-between p-2 border-b border-gray-300">
//                                                                             <span className="font-semibold text-blue-500">{name.trim()}</span>
//                                                                             <span className="text-black-600">{type.trim()}</span>
//                                                                         </div>
//                                                                     );
//                                                                 })}
//                                                             </div>
//                                                             <p className="mt-4 font-semibold">Total Rows: {file.details.num_rows}</p>
//                                                         </div>
//                                                     </td>
//                                                 </tr>
//                                             )}

//                                             {/* File Data with Visualization */}
//                                             {fileDataMap[file.file] && (
//                                                 <tr>
//                                                     <td colSpan={2} className="p-6">
//                                                         <div className={`p-4 rounded-lg shadow-md border-l-4 border-green-500 ${darkMode ? "bg-gray-700 text-white" : "bg-gray-50 text-black"}`}>
//                                                             <div className="flex justify-between items-center mb-2">
//                                                                 <h3 className="text-lg font-semibold">File Data: {fileDataMap[file.file].file}</h3>
//                                                                 <button
//                                                                     className="text-sm text-blue-500 hover:underline"
//                                                                     onClick={() => setFileDataMap(prev => {
//                                                                         const newMap = { ...prev };
//                                                                         delete newMap[file.file];
//                                                                         return newMap;
//                                                                     })}
//                                                                 >
//                                                                     Close
//                                                                 </button>
//                                                             </div>
//                                                             {loadingDataMap[file.file] ? (
//                                                                 <LoadingSpinner />
//                                                             ) : fileDataMap[file.file].error ? (
//                                                                 <p className="text-red-500">{fileDataMap[file.file].error}</p>
//                                                             ) : (
//                                                                 <>
//                                                                     {/* Data Table */}
//                                                                     <div className="overflow-x-auto max-h-96 mb-6">
//                                                                         <table className="w-full border-collapse border border-gray-300">
//                                                                             <thead>
//                                                                                 <tr className={`${darkMode ? "bg-gray-600" : "bg-gray-200"}`}>
//                                                                                     {Object.keys(fileDataMap[file.file].data[0] || {}).map((key) => (
//                                                                                         <th key={key} className="border p-2 sticky top-0 bg-inherit">{key}</th>
//                                                                                     ))}
//                                                                                 </tr>
//                                                                             </thead>
//                                                                             <tbody>
//                                                                                 {fileDataMap[file.file].data.map((row, idx) => (
//                                                                                     <tr key={idx} className={`${darkMode ? "hover:bg-gray-600" : "hover:bg-gray-100"}`}>
//                                                                                         {Object.values(row).map((val, i) => (
//                                                                                             <td key={i} className="border p-2">{val}</td>
//                                                                                         ))}
//                                                                                     </tr>
//                                                                                 ))}
//                                                                             </tbody>
//                                                                         </table>
//                                                                     </div>

//                                                                     {/* Visualization Section */}
//                                                                     <div className="mt-6">
//                                                                         <h4 className="text-lg font-semibold mb-2">Data Visualization</h4>
//                                                                         <select
//                                                                             className="mb-4 p-2 border rounded-md text-black"
//                                                                             value={selectedChartColumn[file.file] || ""}
//                                                                             onChange={(e) => setSelectedChartColumn(prev => ({
//                                                                                 ...prev,
//                                                                                 [file.file]: e.target.value
//                                                                             }))}
//                                                                         >
//                                                                             <option value="">Select a column to visualize</option>
//                                                                             {Object.keys(fileDataMap[file.file].data[0] || {}).map(col => (
//                                                                                 <option key={col} value={col}>{col}</option>
//                                                                             ))}
//                                                                         </select>

//                                                                         {selectedChartColumn[file.file] && (
//                                                                             (() => {
//                                                                                 const chartData = generateChartData(fileDataMap[file.file], selectedChartColumn[file.file]);
//                                                                                 if (!chartData) return <p>No numeric data available for visualization</p>;

//                                                                                 return (
//                                                                                     <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
//                                                                                         <div>
//                                                                                             <h5 className="text-md font-medium mb-2">Bar Chart</h5>
//                                                                                             <Bar
//                                                                                                 data={chartData.bar}
//                                                                                                 options={{
//                                                                                                     responsive: true,
//                                                                                                     plugins: {
//                                                                                                         legend: { position: 'top' },
//                                                                                                         title: { display: true, text: `${selectedChartColumn[file.file]} Distribution` }
//                                                                                                     }
//                                                                                                 }}
//                                                                                             />
//                                                                                         </div>
//                                                                                         <div>
//                                                                                             <h5 className="text-md font-medium mb-2">Pie Chart</h5>
//                                                                                             <Pie
//                                                                                                 data={chartData.pie}
//                                                                                                 options={{
//                                                                                                     responsive: true,
//                                                                                                     plugins: {
//                                                                                                         legend: { position: 'top' },
//                                                                                                         title: { display: true, text: `${selectedChartColumn[file.file]} Distribution` }
//                                                                                                     }
//                                                                                                 }}
//                                                                                             />
//                                                                                         </div>
//                                                                                     </div>
//                                                                                 );
//                                                                             })()
//                                                                         )}
//                                                                     </div>
//                                                                 </>
//                                                             )}
//                                                         </div>
//                                                     </td>
//                                                 </tr>
//                                             )}
//                                         </React.Fragment>
//                                     ))}
//                                 </tbody>
//                             </table>
//                         </div>

//                         {/* Pagination (unchanged) */}
//                         {filteredMetadata.length > itemsPerPage && (
//                             <div className="flex justify-center items-center mt-6 space-x-4">
//                                 <button
//                                     onClick={() => setCurrentPage(p => Math.max(1, p - 1))}
//                                     disabled={currentPage === 1}
//                                     className="px-4 py-2 bg-blue-500 text-white rounded-md disabled:bg-gray-400 disabled:cursor-not-allowed hover:bg-blue-600 focus:outline-none focus:ring-2 focus:ring-blue-300"
//                                 >
//                                     Previous
//                                 </button>
//                                 <span className="text-sm">
//                                     Page {currentPage} of {Math.ceil(filteredMetadata.length / itemsPerPage)}
//                                 </span>
//                                 <button
//                                     onClick={() => setCurrentPage(p => p + 1)}
//                                     disabled={currentPage * itemsPerPage >= filteredMetadata.length}
//                                     className="px-4 py-2 bg-blue-500 text-white rounded-md disabled:bg-gray-400 disabled:cursor-not-allowed hover:bg-blue-600 focus:outline-none focus:ring-2 focus:ring-blue-300"
//                                 >
//                                     Next
//                                 </button>
//                             </div>
//                         )}
//                     </>
//                 )}
//             </div>
//         </ErrorBoundary>
//     );
// };

// export default MetadataViewer;



// frontend/src/components/MetadataViewer.js
import React, { useState, Component } from "react";
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
ChartJS.register(
    CategoryScale,
    LinearScale,
    BarElement,
    Title,
    Tooltip,
    Legend
);

class ErrorBoundary extends Component {
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
    if (["parquet", "delta", "iceberg", "hudi"].includes(ext)) return <FaFileCode className="text-blue-500" />;
    return <FaFileAlt className="text-gray-500" />;
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
    const values = data.map(row => row[column]);
    const isNumeric = values.every(val => !isNaN(Number(val)) && val !== null);

    if (isNumeric) {
        const numericValues = values.map(val => Number(val)).filter(val => !isNaN(val));
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
        // For categorical data, compute the top 5 most frequent values
        const frequency = {};
        values.forEach(val => {
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
    const values = data.map(row => Number(row[column])).filter(val => !isNaN(val));
    if (values.length === 0) return null;

    const min = Math.min(...values);
    const max = Math.max(...values);
    const numBins = 10;
    const binWidth = (max - min) / numBins;
    const bins = Array(numBins).fill(0);

    values.forEach(val => {
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
        datasets: [{
            label: `Distribution of ${column}`,
            data: bins,
            backgroundColor: "rgba(75, 192, 192, 0.6)",
            borderColor: "rgba(75, 192, 192, 1)",
            borderWidth: 1,
        }]
    };
};

// Utility to generate bar chart data for categorical columns
const generateCategoricalBarData = (topValues, column) => {
    const labels = topValues.map(([value]) => value);
    const data = topValues.map(([, count]) => count);

    return {
        labels,
        datasets: [{
            label: `Top Values in ${column}`,
            data,
            backgroundColor: "rgba(54, 162, 235, 0.6)",
            borderColor: "rgba(54, 162, 235, 1)",
            borderWidth: 1,
        }]
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

    const parsePath = (path) => {
        const match = path.match(/^(s3|azure|minio):\/\/([^/]+)(?:\/(.+))?$/);
        if (!match) return { type: null, bucket: null, prefix: "" };
        return { type: match[1], bucket: match[2], prefix: match[3] || "" };
    };

    const fetchMetadata = async () => {
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
                params: { bucket, prefix }
            });
            setMetadata(response.data.files || []);
        } catch (err) {
            setError(err.response?.data?.error || "Failed to fetch metadata");
        } finally {
            setLoading(false);
        }
    };

    const fetchData = async (item) => {
        setLoadingDataMap(prev => ({ ...prev, [item.file]: true }));
        try {
            const response = await axios.get(`http://127.0.0.1:5000/data`, {
                params: { file: item.file, bucket: parsePath(objectStorePath).bucket }
            });
            setDataMap(prev => ({ ...prev, [item.file]: response.data }));
            const columns = Object.keys(response.data.data[0] || {});
            setHiddenColumns(prev => ({
                ...prev,
                [item.file]: columns.reduce((acc, col) => ({ ...acc, [col]: false }), {})
            }));
        } catch (err) {
            setDataMap(prev => ({ ...prev, [item.file]: { error: "Failed to load data" } }));
        } finally {
            setLoadingDataMap(prev => ({ ...prev, [item.file]: false }));
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
        setCurrentPage(1);
    }, 300);

    const togglePinColumn = (file, column) => {
        setPinnedColumns(prev => ({
            ...prev,
            [file]: {
                ...prev[file],
                [column]: !prev[file]?.[column]
            }
        }));
    };

    const toggleHideColumn = (file, column) => {
        setHiddenColumns(prev => ({
            ...prev,
            [file]: {
                ...prev[file],
                [column]: !prev[file]?.[column]
            }
        }));
    };

    const filteredMetadata = metadata.map((item) => {
        const parts = item.file.split(".");
        const filename = parts.slice(0, -1).join(".") || item.file;
        const extension = parts.length > 1 ? parts[parts.length - 1] : item.details.format;
        return { ...item, filename, extension };
    }).filter((item) =>
        item.filename.toLowerCase().includes(searchQuery.toLowerCase())
    );

    const paginatedMetadata = filteredMetadata.slice(
        (currentPage - 1) * itemsPerPage,
        currentPage * itemsPerPage
    );

    const LoadingSpinner = () => (
        <div className="flex justify-center items-center h-32">
            <div className="animate-spin rounded-full h-12 w-12 border-t-2 border-b-2 border-blue-500"></div>
        </div>
    );

    return (
        <ErrorBoundary>
            <div className={`min-h-screen p-6 transition-all ${darkMode ? "bg-gray-900 text-white" : "bg-gray-100 text-black"}`}>
                <form onSubmit={handleSubmit} className="mb-6">
                    <div className="flex flex-col sm:flex-row gap-4 items-center">
                        <input
                            type="text"
                            placeholder="Enter path (e.g., s3://bucket/prefix)"
                            value={objectStorePath}
                            onChange={(e) => setObjectStorePath(e.target.value)}
                            className="px-4 py-2 border rounded-lg w-full sm:w-2/3 shadow-md focus:outline-none focus:ring-2 focus:ring-blue-400 text-black"
                        />
                        <button
                            type="submit"
                            className="px-4 py-2 bg-blue-500 text-white rounded-md hover:bg-blue-600 focus:outline-none focus:ring-2 focus:ring-blue-300"
                            disabled={loading}
                        >
                            Load Metadata
                        </button>
                        <label className="relative inline-flex items-center cursor-pointer">
                            <input
                                type="checkbox"
                                className="sr-only peer"
                                checked={darkMode}
                                onChange={() => setDarkMode(!darkMode)}
                            />
                            <div className="w-14 h-7 bg-gray-300 peer-focus:ring-4 peer-focus:ring-blue-300 rounded-full peer dark:bg-gray-700 peer-checked:after:translate-x-7 peer-checked:after:border-white after:content-[''] after:absolute after:top-0.5 after:left-0.5 after:bg-white after:border after:rounded-full after:h-6 after:w-6 after:transition-all peer-checked:bg-blue-600"></div>
                        </label>
                    </div>
                </form>

                <input
                    type="text"
                    placeholder="Search tables/files..."
                    onChange={(e) => handleSearch(e.target.value)}
                    className="mb-6 px-4 py-2 border rounded-lg w-full shadow-md focus:outline-none focus:ring-2 focus:ring-blue-400 text-black"
                />

                {loading ? (
                    <LoadingSpinner />
                ) : error ? (
                    <p className="text-red-500 font-semibold text-center">{error}</p>
                ) : (
                    <>
                        <div className="overflow-x-auto w-full max-w-6xl mx-auto">
                            <table className={`w-full shadow-lg rounded-lg overflow-hidden ${darkMode ? "bg-gray-800" : "bg-white"}`}>
                                <thead className={darkMode ? "bg-gray-700" : "bg-gray-200"}>
                                    <tr>
                                        <th className="px-6 py-3 text-left font-semibold">Table/File Name</th>
                                        <th className="px-6 py-3 text-left font-semibold">Format</th>
                                        <th className="px-6 py-3 text-left font-semibold">Actions</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {paginatedMetadata.map((item, index) => (
                                        <React.Fragment key={index}>
                                            <tr className={darkMode ? "hover:bg-gray-600" : "hover:bg-gray-100"}>
                                                <td className="px-6 py-4 font-mono flex items-center space-x-2">
                                                    {getFileIcon(item.extension)}
                                                    <span>{item.file}</span>
                                                </td>
                                                <td className="px-6 py-4">{item.details.format}</td>
                                                <td className="px-6 py-4">
                                                    <div className="flex space-x-2">
                                                        <button
                                                            className="px-3 py-2 bg-blue-500 text-white rounded-md hover:bg-blue-600 focus:outline-none focus:ring-2 focus:ring-blue-300"
                                                            onClick={() => toggleExpand(item.file)}
                                                        >
                                                            {expandedItem === item.file ? "Hide Details" : "View Details"}
                                                        </button>
                                                        {item.details.format === "parquet" && (
                                                            <button
                                                                className="px-3 py-2 bg-green-500 text-white rounded-md hover:bg-green-600 focus:outline-none focus:ring-2 focus:ring-green-300"
                                                                onClick={() => {
                                                                    if (dataMap[item.file]) {
                                                                        setDataMap(prev => { const newMap = { ...prev }; delete newMap[item.file]; return newMap; });
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
                                                        <div className={`p-4 rounded-lg shadow-md border-l-4 border-blue-500 ${darkMode ? "bg-gray-700" : "bg-gray-50"}`}>
                                                            <h3 className="text-lg font-semibold mb-2">Schema</h3>
                                                            <div className="grid grid-cols-3 gap-4 text-sm font-mono">
                                                                {item.details.columns.map((col, i) => (
                                                                    <div key={i} className="flex flex-col p-2 border-b border-gray-300">
                                                                        <span className="font-semibold text-blue-500">{col.name}</span>
                                                                        <span>{formatDataType(col.type)}</span>
                                                                        <span className="text-gray-500">{col.nullable ? "Nullable" : "Not Nullable"}</span>
                                                                    </div>
                                                                ))}
                                                            </div>
                                                            <h3 className="text-lg font-semibold mt-4">Partition Details</h3>
                                                            {item.details.partition_keys.length > 0 ? (
                                                                <ul className="list-disc pl-5">
                                                                    {item.details.partition_keys.map((key, i) => <li key={i}>{key}</li>)}
                                                                </ul>
                                                            ) : <p>No partitions</p>}
                                                            <h3 className="text-lg font-semibold mt-4">Snapshots/Versions</h3>
                                                            {item.details.snapshots.length > 0 ? (
                                                                <ul className="list-disc pl-5">
                                                                    {item.details.snapshots.map((snap, i) => (
                                                                        <li key={i}>ID: {snap.id || snap.version}, Timestamp: {snap.timestamp}</li>
                                                                    ))}
                                                                </ul>
                                                            ) : <p>No snapshots</p>}
                                                            <h3 className="text-lg font-semibold mt-4">Key Metrics</h3>
                                                            <p>File Size: {formatFileSize(item.details.file_size)}</p>
                                                            <p>Row Count: {formatNumber(item.details.num_rows)}</p>
                                                        </div>
                                                    </td>
                                                </tr>
                                            )}

                                            {dataMap[item.file] && (
                                                <tr>
                                                    <td colSpan={3} className="p-6">
                                                        <div className={`p-4 rounded-lg shadow-md border-l-4 border-green-500 ${darkMode ? "bg-gray-700 text-white" : "bg-gray-50 text-black"}`}>
                                                            <div className="flex justify-between items-center mb-2">
                                                                <h3 className="text-lg font-semibold">Sample Data: {dataMap[item.file].file}</h3>
                                                                <button
                                                                    className="text-sm text-blue-500 hover:underline"
                                                                    onClick={() => setDataMap(prev => { const newMap = { ...prev }; delete newMap[item.file]; return newMap; })}
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
                                                                        <h4 className="text-md font-semibold mb-2">Manage Columns</h4>
                                                                        <div className="flex flex-wrap gap-2">
                                                                            {Object.keys(dataMap[item.file].data[0] || {}).map((key) => (
                                                                                <div key={key} className="flex items-center space-x-2">
                                                                                    <input
                                                                                        type="checkbox"
                                                                                        checked={!hiddenColumns[item.file]?.[key]}
                                                                                        onChange={() => toggleHideColumn(item.file, key)}
                                                                                    />
                                                                                    <label className="text-sm">{key}</label>
                                                                                    <button
                                                                                        onClick={() => togglePinColumn(item.file, key)}
                                                                                        className={`text-sm px-2 py-1 rounded ${pinnedColumns[item.file]?.[key] ? "bg-blue-500 text-white" : "bg-gray-300 text-black"}`}
                                                                                    >
                                                                                        {pinnedColumns[item.file]?.[key] ? "Unpin" : "Pin"}
                                                                                    </button>
                                                                                </div>
                                                                            ))}
                                                                        </div>
                                                                    </div>
                                                                    <div className="overflow-x-auto max-h-96 mb-6 w-full max-w-6xl mx-auto">
                                                                        <div className="inline-block min-w-full">
                                                                            <table className={`border-collapse border border-gray-300 ${darkMode ? "bg-gray-800" : "bg-white"}`}>
                                                                                <thead>
                                                                                    <tr className={`${darkMode ? "bg-gray-600" : "bg-gray-200"} sticky top-0 z-10`}>
                                                                                        {Object.keys(dataMap[item.file].data[0] || {})
                                                                                            .filter(key => !hiddenColumns[item.file]?.[key])
                                                                                            .map((key, idx) => (
                                                                                                <th
                                                                                                    key={key}
                                                                                                    className={`border p-2 whitespace-nowrap ${pinnedColumns[item.file]?.[key] ? "sticky z-20 bg-inherit" : ""}`}
                                                                                                    style={{
                                                                                                        minWidth: "150px",
                                                                                                        left: pinnedColumns[item.file]?.[key] ? `${Object.keys(dataMap[item.file].data[0] || {})
                                                                                                            .filter(k => !hiddenColumns[item.file]?.[k] && pinnedColumns[item.file]?.[k])
                                                                                                            .slice(0, Object.keys(dataMap[item.file].data[0] || {})
                                                                                                                .filter(k => !hiddenColumns[item.file]?.[k] && pinnedColumns[item.file]?.[k])
                                                                                                                .indexOf(key))
                                                                                                            .reduce((acc, k) => acc + 150, 0)}px` : undefined
                                                                                                    }}
                                                                                                >
                                                                                                    {key}
                                                                                                </th>
                                                                                            ))}
                                                                                    </tr>
                                                                                </thead>
                                                                                <tbody>
                                                                                    {dataMap[item.file].data.map((row, idx) => (
                                                                                        <tr key={idx} className={`${darkMode ? "hover:bg-gray-600" : "hover:bg-gray-100"}`}>
                                                                                            {Object.keys(row)
                                                                                                .filter(key => !hiddenColumns[item.file]?.[key])
                                                                                                .map((key, i) => (
                                                                                                    <td
                                                                                                        key={i}
                                                                                                        className={`border p-2 whitespace-nowrap ${pinnedColumns[item.file]?.[key] ? "sticky z-10 bg-inherit" : ""}`}
                                                                                                        style={{
                                                                                                            minWidth: "150px",
                                                                                                            left: pinnedColumns[item.file]?.[key] ? `${Object.keys(dataMap[item.file].data[0] || {})
                                                                                                                .filter(k => !hiddenColumns[item.file]?.[k] && pinnedColumns[item.file]?.[k])
                                                                                                                .slice(0, Object.keys(dataMap[item.file].data[0] || {})
                                                                                                                    .filter(k => !hiddenColumns[item.file]?.[k] && pinnedColumns[item.file]?.[k])
                                                                                                                    .indexOf(key))
                                                                                                                .reduce((acc, k) => acc + 150, 0)}px` : undefined
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
                                                                    <div className="mt-6">
                                                                        <h4 className="text-lg font-semibold mb-2">Column Statistics</h4>
                                                                        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
                                                                            {Object.keys(dataMap[item.file].data[0] || {}).map((col, idx) => {
                                                                                const stats = computeColumnStats(dataMap[item.file].data, col);
                                                                                return (
                                                                                    <div key={idx} className="p-4 border rounded-lg">
                                                                                        <h5 className="text-md font-medium mb-2">{col}</h5>
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
                                                                                                        <li key={i}>{value}: {count}</li>
                                                                                                    ))}
                                                                                                </ul>
                                                                                            </div>
                                                                                        )}
                                                                                    </div>
                                                                                );
                                                                            })}
                                                                        </div>
                                                                        <h4 className="text-lg font-semibold mb-2 mt-6">Data Distribution</h4>
                                                                        <select
                                                                            className="mb-4 p-2 border rounded-md text-black"
                                                                            value={selectedChartColumn[item.file] || ""}
                                                                            onChange={(e) => setSelectedChartColumn(prev => ({ ...prev, [item.file]: e.target.value }))}
                                                                        >
                                                                            <option value="">Select a column to visualize</option>
                                                                            {Object.keys(dataMap[item.file].data[0] || {}).map(col => (
                                                                                <option key={col} value={col}>{col}</option>
                                                                            ))}
                                                                        </select>
                                                                        {selectedChartColumn[item.file] && (
                                                                            (() => {
                                                                                const stats = computeColumnStats(dataMap[item.file].data, selectedChartColumn[item.file]);
                                                                                if (stats.type === "numeric") {
                                                                                    const histogramData = generateHistogramData(dataMap[item.file].data, selectedChartColumn[item.file]);
                                                                                    if (!histogramData) return <p>No numeric data available for visualization</p>;
                                                                                    return (
                                                                                        <div className="w-full max-w-md mx-auto">
                                                                                            <h5 className="text-md font-medium mb-2">Histogram</h5>
                                                                                            <div style={{ height: "400px", width: "100%" }}>
                                                                                                <Bar
                                                                                                    data={histogramData}
                                                                                                    options={{
                                                                                                        responsive: true,
                                                                                                        maintainAspectRatio: false,
                                                                                                        plugins: {
                                                                                                            legend: { position: "top" },
                                                                                                            title: { display: true, text: `${selectedChartColumn[item.file]} Distribution` }
                                                                                                        },
                                                                                                        scales: {
                                                                                                            x: { ticks: { autoSkip: true, maxRotation: 45, minRotation: 45 } },
                                                                                                            y: { beginAtZero: true }
                                                                                                        }
                                                                                                    }}
                                                                                                />
                                                                                            </div>
                                                                                        </div>
                                                                                    );
                                                                                } else {
                                                                                    const barData = generateCategoricalBarData(stats.topValues, selectedChartColumn[item.file]);
                                                                                    return (
                                                                                        <div className="w-full max-w-md mx-auto">
                                                                                            <h5 className="text-md font-medium mb-2">Top Values</h5>
                                                                                            <div style={{ height: "400px", width: "100%" }}>
                                                                                                <Bar
                                                                                                    data={barData}
                                                                                                    options={{
                                                                                                        responsive: true,
                                                                                                        maintainAspectRatio: false,
                                                                                                        plugins: {
                                                                                                            legend: { position: "top" },
                                                                                                            title: { display: true, text: `Top Values in ${selectedChartColumn[item.file]}` }
                                                                                                        },
                                                                                                        scales: {
                                                                                                            x: { ticks: { autoSkip: true, maxRotation: 45, minRotation: 45 } },
                                                                                                            y: { beginAtZero: true }
                                                                                                        }
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
                                    ))}
                                </tbody>
                            </table>
                        </div>

                        {filteredMetadata.length > itemsPerPage && (
                            <div className="flex justify-center items-center mt-6 space-x-4">
                                <button
                                    onClick={() => setCurrentPage(p => Math.max(1, p - 1))}
                                    disabled={currentPage === 1}
                                    className="px-4 py-2 bg-blue-500 text-white rounded-md disabled:bg-gray-400 disabled:cursor-not-allowed hover:bg-blue-600 focus:outline-none focus:ring-2 focus:ring-blue-300"
                                >
                                    Previous
                                </button>
                                <span className="text-sm">Page {currentPage} of {Math.ceil(filteredMetadata.length / itemsPerPage)}</span>
                                <button
                                    onClick={() => setCurrentPage(p => p + 1)}
                                    disabled={currentPage * itemsPerPage >= filteredMetadata.length}
                                    className="px-4 py-2 bg-blue-500 text-white rounded-md disabled:bg-gray-400 disabled:cursor-not-allowed hover:bg-blue-600 focus:outline-none focus:ring-2 focus:ring-blue-300"
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