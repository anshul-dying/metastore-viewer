import React, { useState, useEffect, useCallback, useMemo } from "react";
import axios from "axios";
import Lottie from 'lottie-react';
import { FaFileAlt, FaFileExcel, FaFilePdf, FaFileImage, FaFileCode, FaTimes } from "react-icons/fa";
import { Bar, Pie } from "react-chartjs-2";
import {
    Chart as ChartJS,
    CategoryScale,
    LinearScale,
    BarElement,
    Title,
    Tooltip,
    Legend,
    ArcElement
} from "chart.js";
import { motion, AnimatePresence } from "framer-motion";
import './MetadataViewer.css'; // Make sure you create this CSS file

// Register ChartJS components
ChartJS.register(
    CategoryScale,
    LinearScale,
    BarElement,
    Title,
    Tooltip,
    Legend,
    ArcElement
);

const loadingContainerStyle = {
    display: 'flex',
    flexDirection: 'column',
    alignItems: 'center',
    justifyContent: 'center',
    height: '300px',
    width: '100%',
    background: 'linear-gradient(135deg, rgba(0,161,214,0.1) 0%, rgba(0,161,214,0.05) 100%)',
    borderRadius: '12px'
};

const PartitionDataModal = ({
    isOpen,
    onClose,
    partition,
    data,
    loading,
    error
}) => {
    if (!isOpen) return null;

    return (
        <div className="fixed inset-0 z-50 flex items-center justify-center overflow-auto bg-black/60 backdrop-blur-sm p-4">
            <div className="bg-white dark:bg-dark-teal rounded-xl shadow-2xl max-w-6xl w-full max-h-[90vh] flex flex-col transform transition-all duration-300 ease-in-out">
                <div className="flex justify-between items-center p-6 border-b border-subtle-gray/20 dark:border-white/10">
                    <h3 className="text-xl font-montserrat font-semibold text-medium-gray dark:text-accent-blue">
                        Partition Data: {partition}
                    </h3>
                    <button
                        onClick={onClose}
                        className="p-2 rounded-full hover:bg-gray-100 dark:hover:bg-gray-700 transition-colors duration-200"
                        aria-label="Close"
                    >
                        <FaTimes className="text-medium-gray dark:text-white" />
                    </button>
                </div>
                <div className="flex-1 overflow-auto p-6">
                    {loading ? (
                        <LoadingSpinner />
                    ) : error ? (
                        <div className="bg-red-50 dark:bg-red-900/20 p-4 rounded-lg border border-red-200 dark:border-red-500/30">
                            <p className="text-red-500">{error}</p>
                        </div>
                    ) : data && data.data ? (
                        <div className="overflow-x-auto rounded-lg border border-subtle-gray/20 dark:border-white/10">
                            <table className="w-full border-collapse bg-white dark:bg-dark-teal">
                                <thead>
                                    <tr className="bg-subtle-gray/50 dark:bg-medium-gray/30">
                                        {Object.keys(data.data[0] || {}).map((key) => (
                                            <th
                                                key={key}
                                                className="p-4 whitespace-nowrap text-medium-gray dark:text-white text-left border-b border-subtle-gray/20 dark:border-white/10"
                                            >
                                                {key}
                                            </th>
                                        ))}
                                    </tr>
                                </thead>
                                <tbody>
                                    {data.data.map((row, idx) => (
                                        <tr
                                            key={idx}
                                            className="hover:bg-accent-blue/5 dark:hover:bg-white/5 transition-all duration-200"
                                        >
                                            {Object.values(row).map((value, i) => (
                                                <td
                                                    key={i}
                                                    className="p-4 whitespace-nowrap text-medium-gray dark:text-white border-b border-subtle-gray/20 dark:border-white/10"
                                                >
                                                    {value !== null ? value.toString() : 'NULL'}
                                                </td>
                                            ))}
                                        </tr>
                                    ))}
                                </tbody>
                            </table>
                        </div>
                    ) : (
                        <div className="text-center py-8">
                            <p className="text-medium-gray dark:text-white/70">No data available for this partition</p>
                        </div>
                    )}
                </div>
            </div>
        </div>
    );
};

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

// Enhanced file icon handling
const getFileIcon = (extension, format) => {
    if (format === "delta") return <FaFileCode className="text-blue-500" />;
    if (format === "iceberg") return <FaFileCode className="text-purple-500" />;
    if (format === "hudi") return <FaFileCode className="text-orange-500" />;

    const ext = extension.toLowerCase();
    if (["xls", "xlsx", "csv"].includes(ext)) return <FaFileExcel className="text-green-500" />;
    if (["pdf"].includes(ext)) return <FaFilePdf className="text-red-500" />;
    if (["jpg", "jpeg", "png", "gif"].includes(ext)) return <FaFileImage className="text-yellow-500" />;
    if (["parquet", "avro", "orc"].includes(ext)) return <FaFileCode className="text-accent-blue" />;
    return <FaFileAlt className="text-subtle-gray dark:text-white" />;
};

// Utility functions
const debounce = (func, delay) => {
    let timeoutId;
    return (...args) => {
        clearTimeout(timeoutId);
        timeoutId = setTimeout(() => func(...args), delay);
    };
};

const formatNumber = (num) => {
    return num.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
};

const formatFileSize = (bytes) => {
    if (bytes === 0) return "0 Bytes";
    const k = 1024;
    const sizes = ["Bytes", "KB", "MB", "GB", "TB"];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + " " + sizes[i];
};

const formatDataType = (type) => {
    if (type.includes("date32[day]")) return "Date";
    if (type.includes("int")) return "Integer";
    if (type.includes("string")) return "String";
    return type;
};

const formatPartitionDisplay = (partition) => {
    return partition.split('/').map((part, index) => {
        const [key, value] = part.split('=');
        return (
            <div key={index} className="flex items-center">
                <span className="font-semibold text-accent-blue mr-1">{key}:</span>
                <span className="text-medium-gray dark:text-white">{value}</span>
            </div>
        );
    });
};

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
                backgroundColor: "rgba(0, 161, 214, 0.6)",
                borderColor: "#00A1D6",
                borderWidth: 1,
            },
        ],
    };
};

const generateCategoricalBarData = (topValues, column) => {
    const labels = topValues.map(([value]) => value);
    const data = topValues.map(([, count]) => count);

    return {
        labels,
        datasets: [
            {
                label: `Top Values in ${column}`,
                data,
                backgroundColor: "rgba(0, 161, 214, 0.6)",
                borderColor: "#00A1D6",
                borderWidth: 1,
            },
        ],
    };
};

const generatePartitionPieData = (partitions) => {
    const partitionCounts = partitions.reduce((acc, partition) => {
        const key = partition.split('/').pop();
        acc[key] = (acc[key] || 0) + 1;
        return acc;
    }, {});

    const labels = Object.keys(partitionCounts);
    const data = Object.values(partitionCounts);

    const backgroundColors = [
        'rgba(0, 161, 214, 0.6)',
        'rgba(76, 175, 80, 0.6)',
        'rgba(255, 152, 0, 0.6)',
        'rgba(156, 39, 176, 0.6)',
        'rgba(244, 67, 54, 0.6)',
        'rgba(33, 150, 243, 0.6)',
        'rgba(255, 235, 59, 0.6)',
    ];

    return {
        labels,
        datasets: [
            {
                data,
                backgroundColor: backgroundColors.slice(0, labels.length),
                borderColor: 'rgba(255, 255, 255, 0.8)',
                borderWidth: 1,
            },
        ],
    };
};

const Modal = ({ isOpen, onClose, title, children, size = "lg" }) => {
    if (!isOpen) return null;

    const sizeClasses = {
        sm: "max-w-lg",
        md: "max-w-2xl",
        lg: "max-w-4xl",
        xl: "max-w-6xl",
        full: "max-w-full mx-6"
    };

    return (
        <motion.div
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            transition={{ duration: 0.3 }}
            className="fixed inset-0 z-50 flex items-center justify-center overflow-auto bg-black/60 backdrop-blur-sm p-4"
        >
            <motion.div
                initial={{ scale: 0.9, opacity: 0 }}
                animate={{ scale: 1, opacity: 1 }}
                exit={{ scale: 0.9, opacity: 0 }}
                transition={{ type: "spring", stiffness: 300, damping: 30 }}
                className={`bg-white/95 dark:bg-dark-teal/95 rounded-2xl shadow-2xl ${sizeClasses[size]} w-full max-h-[90vh] flex flex-col relative overflow-hidden`}
            >
                <div className="absolute top-0 left-0 w-full h-1 bg-gradient-to-r from-accent-blue via-blue-500 to-purple-600"></div>
                <div className="flex justify-between items-center p-6 border-b border-subtle-gray/10 dark:border-white/5">
                    <h3 className="text-xl font-montserrat font-semibold bg-gradient-to-r from-accent-blue to-blue-600 bg-clip-text text-transparent">{title}</h3>
                    <motion.button
                        whileHover={{ rotate: 90, scale: 1.1 }}
                        whileTap={{ scale: 0.9 }}
                        onClick={onClose}
                        className="p-2 rounded-full hover:bg-gray-100 dark:hover:bg-gray-700 transition-colors"
                    >
                        <FaTimes className="text-medium-gray dark:text-white" />
                    </motion.button>
                </div>
                <div className="flex-1 overflow-auto p-6 custom-scrollbar">
                    {children}
                </div>
            </motion.div>
        </motion.div>
    );
};

const Tabs = ({ tabs, activeTab, setActiveTab }) => {
    return (
        <div className="mb-8">
            <div className="flex flex-wrap border-b border-subtle-gray/10 dark:border-white/5 relative">
                {tabs.map((tab) => (
                    <motion.button
                        key={tab.id}
                        className={`px-6 py-3 font-montserrat font-medium transition-all duration-300 relative ${activeTab === tab.id
                            ? "text-accent-blue"
                            : "text-medium-gray dark:text-white hover:text-accent-blue"
                            }`}
                        onClick={() => setActiveTab(tab.id)}
                        whileHover={{ y: -2 }}
                        whileTap={{ y: 0 }}
                    >
                        {tab.label}
                        {activeTab === tab.id && (
                            <motion.div
                                layoutId="activeTabIndicator"
                                className="absolute bottom-0 left-0 right-0 h-0.5 bg-gradient-to-r from-accent-blue to-blue-600 rounded-full"
                                initial={false}
                                transition={{ type: "spring", stiffness: 500, damping: 30 }}
                            />
                        )}
                    </motion.button>
                ))}
            </div>
        </div>
    );
};

const LoadingSpinner = ({ withLottie = true }) => {
    if (withLottie) {
        return (
            <motion.div
                initial={{ opacity: 0 }}
                animate={{ opacity: 1 }}
                exit={{ opacity: 0 }}
                className="flex flex-col items-center justify-center p-12 glass-card rounded-2xl enhanced-shadow"
                style={{ minHeight: '400px' }}
            >
                <div className="relative">
                    <Lottie
                        animationData={require('../assets/hand-animation.json')}
                        loop={true}
                        style={{ width: 200, height: 200 }}
                    />
                    <div className="absolute inset-0 breathing-highlight rounded-full"></div>
                </div>
                <motion.div
                    initial={{ opacity: 0, y: 20 }}
                    animate={{ opacity: 1, y: 0 }}
                    transition={{ delay: 0.5, duration: 0.5 }}
                    className="mt-8 text-center"
                >
                    <h3 className="text-xl font-semibold text-gradient mb-2">Discovering Your Data</h3>
                    <p className="text-medium-gray/70 dark:text-white/70">
                        We're preparing your data lake visualization...
                    </p>
                </motion.div>
                <div className="mt-8 grid grid-cols-3 gap-3">
                    {[1, 2, 3].map((i) => (
                        <motion.div
                            key={i}
                            initial={{ scale: 0.8, opacity: 0 }}
                            animate={{
                                scale: [0.8, 1, 0.8],
                                opacity: [0.5, 1, 0.5]
                            }}
                            transition={{
                                duration: 2,
                                repeat: Infinity,
                                delay: i * 0.3
                            }}
                            className="w-3 h-3 rounded-full bg-accent-blue/60"
                        />
                    ))}
                </div>
            </motion.div>
        );
    }

    return (
        <div className="flex flex-col justify-center items-center h-60 glass-card rounded-2xl p-8">
            <div className="relative">
                <svg className="w-16 h-16 animate-spin" viewBox="0 0 24 24">
                    <circle
                        className="opacity-20"
                        cx="12"
                        cy="12"
                        r="10"
                        stroke="currentColor"
                        strokeWidth="3"
                        fill="none"
                    />
                    <path
                        className="text-accent-blue"
                        fill="currentColor"
                        d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"
                    />
                </svg>
                <div className="absolute top-0 left-0 w-16 h-16 rounded-full animate-pulse opacity-30 bg-accent-blue/20"></div>
            </div>
            <p className="text-medium-gray dark:text-white/70 mt-4 font-medium">Processing...</p>
        </div>
    );
};

const StatCard = ({ title, value, icon, color = "blue" }) => {
    const colors = {
        blue: "bg-gradient-to-br from-blue-500/20 to-blue-600/10 text-blue-600 border-blue-300/50",
        green: "bg-gradient-to-br from-green-500/20 to-green-600/10 text-green-600 border-green-300/50",
        red: "bg-gradient-to-br from-red-500/20 to-red-600/10 text-red-600 border-red-300/50",
        yellow: "bg-gradient-to-br from-yellow-500/20 to-yellow-600/10 text-yellow-600 border-yellow-300/50",
        purple: "bg-gradient-to-br from-purple-500/20 to-purple-600/10 text-purple-600 border-purple-300/50",
    };

    return (
        <motion.div
            whileHover={{ y: -5, boxShadow: "0 10px 25px -5px rgba(0,0,0,0.1), 0 10px 10px -5px rgba(0,0,0,0.04)" }}
            transition={{ duration: 0.2 }}
            className={`p-6 rounded-2xl ${colors[color]} border backdrop-blur-sm shadow-xl relative overflow-hidden group`}
        >
            <div className="absolute -right-6 -top-6 w-16 h-16 rounded-full bg-white/10 group-hover:scale-150 transition-all duration-700"></div>
            <div className="absolute right-10 bottom-6 w-8 h-8 rounded-full bg-white/10 group-hover:scale-150 transition-all duration-700 delay-100"></div>
            <div className="flex items-center justify-between z-10 relative">
                <div>
                    <p className="text-sm font-medium opacity-80">{title}</p>
                    <p className="text-3xl font-bold mt-2">{value}</p>
                </div>
                {icon && <div className="text-3xl opacity-80">{icon}</div>}
            </div>
        </motion.div>
    );
};

const MetadataViewer = ({ darkMode }) => {
    const [objectStorePath, setObjectStorePath] = useState("s3://test-bucket");
    const [metadata, setMetadata] = useState([]);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState(null);
    const [searchQuery, setSearchQuery] = useState("");
    const [partitionSearchQuery, setPartitionSearchQuery] = useState("");
    const [dataMap, setDataMap] = useState({});
    const [loadingDataMap, setLoadingDataMap] = useState({});
    const [currentPage, setCurrentPage] = useState(1);
    const [selectedChartColumn, setSelectedChartColumn] = useState({});
    const [pinnedColumns, setPinnedColumns] = useState({});
    const [hiddenColumns, setHiddenColumns] = useState({});
    const [detailsModalOpen, setDetailsModalOpen] = useState(false);
    const [dataModalOpen, setDataModalOpen] = useState(false);
    const [selectedItem, setSelectedItem] = useState(null);
    const [activeTab, setActiveTab] = useState("schema");
    const [selectedSnapshot, setSelectedSnapshot] = useState(null);
    const [selectedPartition, setSelectedPartition] = useState(null);
    const [partitionData, setPartitionData] = useState(null);
    const [loadingPartitionData, setLoadingPartitionData] = useState(false);
    const [snapshotChanges, setSnapshotChanges] = useState(null);
    const [loadingSnapshotChanges, setLoadingSnapshotChanges] = useState(false);
    const [partitionModalOpen, setPartitionModalOpen] = useState(false);
    const [partitionsPage, setPartitionsPage] = useState(1);
    const partitionsPerPage = 15;

    const itemsPerPage = 10;

    const getCurrentPartitions = useMemo(() => {
        if (!selectedItem?.details?.partitions) return [];

        const startIndex = (partitionsPage - 1) * partitionsPerPage;
        const endIndex = startIndex + partitionsPerPage;

        return selectedItem.details.partitions
            .filter(partition =>
                partitionSearchQuery
                    ? partition.toLowerCase().includes(partitionSearchQuery.toLowerCase())
                    : true
            )
            .slice(startIndex, endIndex);
    }, [selectedItem, partitionsPage, partitionSearchQuery]);

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
        setSelectedItem(null);
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
            setMetadata(response.data.files || []);
            setCurrentPage(1);
        } catch (err) {
            setError(err.response?.data?.error || "Failed to fetch metadata");
            setMetadata([]);
        } finally {
            setLoading(false);
        }
    }, [objectStorePath]);

    const fetchData = async (item, version = null, partition = null) => {
        setLoadingDataMap((prev) => ({ ...prev, [item.file]: true }));
        try {
            const params = {
                file: item.file,
                bucket: parsePath(objectStorePath).bucket
            };

            if (version !== null) {
                params.version = version.version || version.id || version;
            }

            const response = await axios.get("http://127.0.0.1:5000/data", { params });
            let data = response.data;

            if (partition) {
                const partitionFilters = partition.split("/").reduce((acc, part) => {
                    const [key, value] = part.split("=");
                    acc[key] = value;
                    return acc;
                }, {});
                data.data = data.data.filter(row =>
                    Object.entries(partitionFilters).every(([key, value]) => row[key] === value)
                );
            }

            setDataMap((prev) => ({
                ...prev,
                [item.file]: {
                    ...data,
                    currentVersion: version || 'latest'
                }
            }));

            const columns = Object.keys(data.data[0] || {});
            setHiddenColumns((prev) => ({
                ...prev,
                [item.file]: columns.reduce((acc, col) => ({ ...acc, [col]: false }), {}),
            }));

            if (partition) setPartitionData(data);
        } catch (err) {
            setDataMap((prev) => ({
                ...prev,
                [item.file]: {
                    error: "Failed to load data",
                    currentVersion: version || 'latest'
                }
            }));
            if (partition) setPartitionData({ error: "Failed to load partition data" });
        } finally {
            setLoadingDataMap((prev) => ({ ...prev, [item.file]: false }));
            setLoadingPartitionData(false);
        }
    };

    const fetchPartitionData = async (item, partition) => {
        setLoadingPartitionData(true);
        try {
            const { bucket } = parsePath(objectStorePath);
            const response = await axios.get("http://127.0.0.1:5000/partition_data", {
                params: {
                    file: item.file,
                    bucket,
                    partition
                }
            });
            setPartitionData(response.data);
        } catch (err) {
            setPartitionData({ error: err.response?.data?.error || "Failed to fetch partition data" });
        } finally {
            setLoadingPartitionData(false);
        }
    };

    const fetchSnapshotChanges = async (item, snapshot) => {
        setLoadingSnapshotChanges(true);
        setSnapshotChanges(null);
        try {
            const response = await axios.get("http://127.0.0.1:5000/snapshot_changes", {
                params: {
                    file: item.file,
                    bucket: parsePath(objectStorePath).bucket,
                    version: snapshot.version
                }
            });
            setSnapshotChanges(response.data);
        } catch (err) {
            setSnapshotChanges({ error: err.response?.data?.error || "Failed to fetch snapshot changes" });
        } finally {
            setLoadingSnapshotChanges(false);
        }
    };

    const handleSubmit = (e) => {
        e.preventDefault();
        fetchMetadata();
    };

    const openDetailsModal = (item) => {
        setSelectedItem(item);
        setDetailsModalOpen(true);
        setActiveTab("schema");
        setSelectedSnapshot(null);
        setSelectedPartition(null);
        setPartitionData(null);
        setSnapshotChanges(null);
        setPartitionSearchQuery("");
        setPartitionsPage(1);
    };

    const openDataModal = (item) => {
        setSelectedItem(item);
        if (!dataMap[item.file]) {
            fetchData(item);
        }
        setDataModalOpen(true);
        setActiveTab("preview");
    };

    const handleSearch = debounce((value) => {
        setSearchQuery(value);
        setCurrentPage(1);
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

    const handlePartitionClick = (partition) => {
        // If clicking a different partition than currently selected
        if (selectedPartition !== partition) {
            setPartitionsPage(1); // Reset to first page
        }
        setSelectedPartition(partition);
        setPartitionModalOpen(true);
        fetchPartitionData(selectedItem, partition);
    };

    const handleSnapshotClick = (snapshot) => {
        setSelectedSnapshot(snapshot);
        fetchData(selectedItem, snapshot);
        fetchSnapshotChanges(selectedItem, snapshot);
    };

    const filteredMetadata = useMemo(() => {
        if (!Array.isArray(metadata)) return [];
        return metadata
            .map((item) => {
                const parts = item.file.split(".");
                const filename = parts.slice(0, -1).join(".") || item.file;
                const extension = parts.length > 1 ? parts[parts.length - 1] : "";
                const format = item.details?.format || "";
                return {
                    ...item,
                    filename,
                    extension,
                    displayFormat: format || extension || "file",
                    isTableFormat: ["delta", "iceberg", "hudi"].includes(format.toLowerCase())
                };
            })
            .filter((item) => item.filename.toLowerCase().includes(searchQuery.toLowerCase()));
    }, [metadata, searchQuery]);

    const paginatedMetadata = useMemo(() => {
        return filteredMetadata.slice((currentPage - 1) * itemsPerPage, currentPage * itemsPerPage);
    }, [filteredMetadata, currentPage]);

    useEffect(() => {
        const maxPage = Math.ceil(filteredMetadata.length / itemsPerPage);
        if (currentPage > maxPage && maxPage > 0) {
            setCurrentPage(maxPage);
        }
    }, [filteredMetadata, currentPage]);

    const renderModalContent = (type) => {
        if (!selectedItem) return null;

        const isDetails = type === "details";
        const fileData = dataMap[selectedItem.file];

        const detailsTabs = [
            { id: "schema", label: "Schema" },
            { id: "partitions", label: "Partitions" },
            { id: "snapshots", label: "Snapshots" },
            { id: "metrics", label: "Metrics" },
            ...(selectedItem.details?.partitions?.length > 0 ? [{ id: "visualizePartitions", label: "Visualize Partitions" }] : []),
        ];

        const dataTabs = [
            { id: "preview", label: "Data Preview" },
            { id: "columns", label: "Column Management" },
            { id: "stats", label: "Statistics" },
            { id: "visualization", label: "Visualization" },
        ];

        const tabs = isDetails ? detailsTabs : dataTabs;

        return (
            <div>
                <Tabs tabs={tabs} activeTab={activeTab} setActiveTab={setActiveTab} />
                {isDetails ? (
                    <>
                        {selectedItem.details && selectedItem.details.error && (
                            <p className="text-red-500 mb-4">Error: {selectedItem.details.error}</p>
                        )}
                        {activeTab === "schema" && (
                            <div>
                                <h4 className="text-lg font-montserrat font-semibold mb-4 text-medium-gray dark:text-accent-blue">Schema</h4>
                                {selectedItem.details && Array.isArray(selectedItem.details.columns) && selectedItem.details.columns.length > 0 ? (
                                    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                                        {selectedItem.details.columns.map((col, i) => (
                                            <div key={i} className="p-3 rounded-md border border-subtle-gray dark:border-white/20 bg-white/50 dark:bg-dark-teal/50">
                                                <div className="font-semibold text-accent-blue">{col.name}</div>
                                                <div className="text-medium-gray dark:text-white mt-1">{formatDataType(col.type)}</div>
                                                <div className="text-xs text-medium-gray dark:text-accent-blue mt-1">
                                                    {col.nullable ? "Nullable" : "Not Nullable"}
                                                </div>
                                            </div>
                                        ))}
                                    </div>
                                ) : (
                                    <p className="text-medium-gray dark:text-white">No schema available</p>
                                )}
                            </div>
                        )}
                        {activeTab === "partitions" && (
                            <div>
                                <h4 className="text-lg font-montserrat font-semibold mb-4 text-medium-gray dark:text-accent-blue">Partition Details</h4>
                                {selectedItem.details && Array.isArray(selectedItem.details.partitions) && selectedItem.details.partitions.length > 0 ? (
                                    <div className="space-y-4">
                                        <div className="mb-4">
                                            <input
                                                type="text"
                                                placeholder="Filter partitions (e.g., country=UK)"
                                                value={partitionSearchQuery}
                                                onChange={(e) => {
                                                    setPartitionSearchQuery(e.target.value);
                                                    setSelectedPartition(null);
                                                    setPartitionData(null);
                                                }}
                                                className="w-full p-2 rounded-lg border border-subtle-gray dark:border-white/20 bg-white dark:bg-dark-teal text-medium-gray dark:text-white focus:outline-none focus:ring-2 focus:ring-accent-blue transition-all duration-300"
                                            />
                                        </div>
                                        <div className="flex flex-wrap gap-2">
                                            {getCurrentPartitions.map((partition, i) => (
                                                <motion.button
                                                    key={i}
                                                    whileHover={{ scale: 1.05 }}
                                                    whileTap={{ scale: 0.95 }}
                                                    className={`p-3 rounded-md border border-subtle-gray dark:border-white/20 bg-white/50 dark:bg-dark-teal/50 cursor-pointer ${selectedPartition === partition ? "bg-accent-blue/20 border-accent-blue" : ""}`}
                                                    onClick={() => handlePartitionClick(partition)}
                                                >
                                                    <div className="flex flex-col space-y-1">
                                                        {formatPartitionDisplay(partition)}
                                                    </div>
                                                </motion.button>
                                            ))}
                                        </div>
                                        {selectedItem.details.partitions.length > partitionsPerPage && (
                                            <div className="flex justify-center mt-4">
                                                <div className="flex gap-2">
                                                    <button
                                                        onClick={() => {
                                                            setPartitionsPage(prev => Math.max(prev - 1, 1));
                                                            setSelectedPartition(null);
                                                        }}
                                                        disabled={partitionsPage === 1}
                                                        className={`px-3 py-1 rounded-md ${partitionsPage === 1
                                                            ? "bg-subtle-gray dark:bg-medium-gray/30 text-medium-gray/50 dark:text-white/50 cursor-not-allowed"
                                                            : "bg-accent-blue/10 text-accent-blue hover:bg-accent-blue/20"
                                                            } transition-colors`}
                                                    >
                                                        Previous
                                                    </button>
                                                    <span className="px-3 py-1 bg-subtle-gray dark:bg-medium-gray/30 text-medium-gray dark:text-white rounded-md">
                                                        {partitionsPage}
                                                    </span>
                                                    <button
                                                        onClick={() => {
                                                            setPartitionsPage(prev => prev + 1);
                                                            setSelectedPartition(null);
                                                        }}
                                                        disabled={partitionsPage >= Math.ceil(selectedItem.details.partitions.length / partitionsPerPage)}
                                                        className={`px-3 py-1 rounded-md ${partitionsPage >= Math.ceil(selectedItem.details.partitions.length / partitionsPerPage)
                                                            ? "bg-subtle-gray dark:bg-medium-gray/30 text-medium-gray/50 dark:text-white/50 cursor-not-allowed"
                                                            : "bg-accent-blue/10 text-accent-blue hover:bg-accent-blue/20"
                                                            } transition-colors`}
                                                    >
                                                        Next
                                                    </button>
                                                </div>
                                            </div>
                                        )}
                                        <PartitionDataModal
                                            isOpen={partitionModalOpen}
                                            onClose={() => setPartitionModalOpen(false)}
                                            partition={selectedPartition}
                                            data={partitionData}
                                            loading={loadingPartitionData}
                                            error={partitionData?.error}
                                        />
                                    </div>
                                ) : (
                                    <p className="text-medium-gray dark:text-white">No partitions</p>
                                )}
                            </div>
                        )}
                        {activeTab === "visualizePartitions" && (
                            <div>
                                <h4 className="text-lg font-montserrat font-semibold mb-4 text-medium-gray dark:text-accent-blue">
                                    Partition Visualization
                                </h4>
                                {selectedItem.details?.partitions?.length > 0 ? (
                                    <div className="space-y-6">
                                        <div className="bg-white dark:bg-dark-teal/50 rounded-lg p-4">
                                            <h5 className="text-md font-semibold text-medium-gray dark:text-accent-blue mb-4">
                                                Partition Distribution
                                            </h5>
                                            <div className="h-64">
                                                <Pie
                                                    data={generatePartitionPieData(selectedItem.details.partitions)}
                                                    options={{
                                                        responsive: true,
                                                        maintainAspectRatio: false,
                                                        plugins: {
                                                            legend: {
                                                                position: 'right',
                                                                labels: {
                                                                    color: darkMode ? 'white' : 'black'
                                                                }
                                                            },
                                                            title: {
                                                                display: true,
                                                                text: 'Partition Distribution',
                                                                color: darkMode ? 'white' : 'black'
                                                            }
                                                        }
                                                    }}
                                                />
                                            </div>
                                        </div>
                                        <div className="bg-white dark:bg-dark-teal/50 rounded-lg p-4">
                                            <h5 className="text-md font-semibold text-medium-gray dark:text-accent-blue mb-4">
                                                Explore Partition Data
                                            </h5>
                                            <select
                                                className="mb-4 p-2 border rounded-md w-full max-w-md bg-white dark:bg-dark-teal border-subtle-gray dark:border-white/20 text-medium-gray dark:text-white"
                                                value={selectedPartition || ""}
                                                onChange={(e) => {
                                                    const partition = e.target.value;
                                                    setSelectedPartition(partition);
                                                    if (partition) fetchPartitionData(selectedItem, partition);
                                                }}
                                            >
                                                <option value="">Select a partition to explore</option>
                                                {selectedItem.details.partitions.map((partition, i) => (
                                                    <option key={i} value={partition}>
                                                        {partition}
                                                    </option>
                                                ))}
                                            </select>
                                            {selectedPartition && loadingPartitionData && <LoadingSpinner />}
                                            {selectedPartition && partitionData && partitionData.data && (
                                                <div className="mt-4">
                                                    <h6 className="text-sm font-semibold text-medium-gray dark:text-white mb-2">
                                                        Column Distributions for {selectedPartition}
                                                    </h6>
                                                    <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                                                        {Object.keys(partitionData.data[0] || {}).map((col) => {
                                                            const stats = computeColumnStats(partitionData.data, col);
                                                            if (stats.type === "numeric") {
                                                                const histogramData = generateHistogramData(partitionData.data, col);
                                                                return histogramData ? (
                                                                    <div key={col} className="bg-white dark:bg-dark-teal/70 p-3 rounded-lg">
                                                                        <div className="h-48">
                                                                            <Bar
                                                                                data={histogramData}
                                                                                options={{
                                                                                    responsive: true,
                                                                                    maintainAspectRatio: false,
                                                                                    plugins: {
                                                                                        legend: { display: false },
                                                                                        title: {
                                                                                            display: true,
                                                                                            text: col,
                                                                                            color: darkMode ? 'white' : 'black'
                                                                                        }
                                                                                    },
                                                                                    scales: {
                                                                                        x: {
                                                                                            ticks: { color: darkMode ? 'white' : 'black' },
                                                                                            grid: { color: darkMode ? 'rgba(255, 255, 255, 0.1)' : 'rgba(0, 0, 0, 0.1)' }
                                                                                        },
                                                                                        y: {
                                                                                            ticks: { color: darkMode ? 'white' : 'black' },
                                                                                            grid: { color: darkMode ? 'rgba(255, 255, 255, 0.1)' : 'rgba(0, 0, 0, 0.1)' }
                                                                                        }
                                                                                    }
                                                                                }}
                                                                            />
                                                                        </div>
                                                                    </div>
                                                                ) : null;
                                                            }
                                                            return null;
                                                        })}
                                                    </div>
                                                </div>
                                            )}
                                        </div>
                                    </div>
                                ) : (
                                    <p className="text-medium-gray dark:text-white">No partitions available to visualize</p>
                                )}
                            </div>
                        )}
                        {activeTab === "snapshots" && (
                            <div>
                                <h4 className="text-lg font-montserrat font-semibold mb-4 text-medium-gray dark:text-accent-blue">
                                    Snapshots/Versions
                                </h4>
                                {selectedItem.details?.snapshots?.length > 0 ? (
                                    <div className="space-y-4">
                                        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3">
                                            {selectedItem.details.snapshots.map((snap, i) => (
                                                <motion.div
                                                    key={i}
                                                    whileHover={{ scale: 1.02 }}
                                                    whileTap={{ scale: 0.98 }}
                                                    className={`p-4 rounded-lg border cursor-pointer transition-all ${selectedSnapshot?.version === snap.version
                                                        ? "border-accent-blue bg-accent-blue/10"
                                                        : "border-subtle-gray dark:border-white/20 bg-white/50 dark:bg-dark-teal/50"
                                                        }`}
                                                    onClick={() => handleSnapshotClick(snap)}
                                                >
                                                    <div className="flex justify-between items-start">
                                                        <div>
                                                            <div className="font-semibold text-accent-blue">
                                                                Version {snap.version}
                                                            </div>
                                                            <div className="text-sm text-medium-gray dark:text-white mt-1">
                                                                {new Date(
                                                                    snap.timestamp.length === 14
                                                                        ? `${snap.timestamp.slice(0, 4)}-${snap.timestamp.slice(4, 6)}-${snap.timestamp.slice(6, 8)} ${snap.timestamp.slice(8, 10)}:${snap.timestamp.slice(10, 12)}:${snap.timestamp.slice(12, 14)}`
                                                                        : snap.timestamp
                                                                ).toLocaleString()}
                                                            </div>
                                                        </div>
                                                        {snap.is_current && selectedSnapshot?.version === snap.version && (
                                                            <span className="text-xs bg-green-500/10 text-green-500 px-2 py-1 rounded-full">
                                                                Current
                                                            </span>
                                                        )}
                                                    </div>
                                                    {/* Rest of the snapshot card content remains the same */}
                                                    <div className="mt-2">
                                                        <div className="text-xs text-medium-gray dark:text-white">
                                                            Operation: <span className="font-medium">{snap.operation}</span>
                                                        </div>
                                                        {snap.operation_metrics && (
                                                            <div className="mt-1 text-xs">
                                                                {Object.entries(snap.operation_metrics).map(([key, value]) => (
                                                                    <div key={key} className="flex justify-between">
                                                                        <span className="text-medium-gray dark:text-white">
                                                                            {key.replace(/_/g, " ")}:
                                                                        </span>
                                                                        <span className="font-medium text-accent-blue">
                                                                            {value}
                                                                        </span>
                                                                    </div>
                                                                ))}
                                                            </div>
                                                        )}
                                                    </div>
                                                </motion.div>
                                            ))}
                                        </div>
                                        {selectedSnapshot && (
                                            <motion.div
                                                initial={{ opacity: 0, height: 0 }}
                                                animate={{ opacity: 1, height: "auto" }}
                                                exit={{ opacity: 0, height: 0 }}
                                                transition={{ duration: 0.3 }}
                                                className="mt-4 p-4 rounded-lg bg-white dark:bg-dark-teal/50 border border-accent-blue/30"
                                            >
                                                <div className="flex justify-between items-start mb-4">
                                                    <div>
                                                        <h5 className="text-lg font-semibold text-accent-blue">
                                                            Version {selectedSnapshot.version} Details
                                                        </h5>
                                                        <div className="flex items-center mt-1 space-x-2">
                                                            <span className="text-xs px-2 py-1 rounded-full bg-accent-blue/10 text-accent-blue">
                                                                {selectedSnapshot.operation}
                                                            </span>
                                                            <span className="text-xs text-medium-gray dark:text-white">
                                                                {new Date(
                                                                    selectedSnapshot.timestamp.length === 14
                                                                        ? `${selectedSnapshot.timestamp.slice(0, 4)}-${selectedSnapshot.slice(4, 6)}-${selectedSnapshot.timestamp.slice(6, 8)} ${selectedSnapshot.timestamp.slice(8, 10)}:${selectedSnapshot.timestamp.slice(10, 12)}:${selectedSnapshot.timestamp.slice(12, 14)}`
                                                                        : selectedSnapshot.timestamp
                                                                ).toLocaleString()}
                                                            </span>
                                                        </div>
                                                    </div>
                                                    {selectedSnapshot.is_current && (
                                                        <span className="text-xs bg-green-500/10 text-green-500 px-2 py-1 rounded-full">
                                                            Current Version
                                                        </span>
                                                    )}
                                                </div>
                                                <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-6">
                                                    <div className="space-y-2">
                                                        <h6 className="text-sm font-semibold text-medium-gray dark:text-white">Operation Details</h6>
                                                        {selectedSnapshot.operation_parameters && (
                                                            <div className="text-sm space-y-1">
                                                                {Object.entries(selectedSnapshot.operation_parameters).map(([key, value]) => (
                                                                    <div key={key} className="flex">
                                                                        <span className="text-medium-gray dark:text-white/80 w-24 truncate">{key}:</span>
                                                                        <span className="text-accent-blue flex-1">
                                                                            {typeof value === 'string' ? value : JSON.stringify(value)}
                                                                        </span>
                                                                    </div>
                                                                ))}
                                                            </div>
                                                        )}
                                                    </div>
                                                    {selectedSnapshot.operation_metrics && (
                                                        <div>
                                                            <h6 className="text-sm font-semibold text-medium-gray dark:text-white mb-2">Metrics</h6>
                                                            <div className="grid grid-cols-2 gap-2">
                                                                {Object.entries(selectedSnapshot.operation_metrics).map(([key, value]) => (
                                                                    <div key={key} className="bg-subtle-gray/10 dark:bg-white/5 p-2 rounded">
                                                                        <p className="text-xs text-medium-gray dark:text-white/80 capitalize">
                                                                            {key.replace(/_/g, ' ')}
                                                                        </p>
                                                                        <p className="text-sm font-medium text-accent-blue">
                                                                            {value}
                                                                        </p>
                                                                    </div>
                                                                ))}
                                                            </div>
                                                        </div>
                                                    )}
                                                </div>
                                                <div className="border-t border-subtle-gray/20 dark:border-white/10 pt-4">
                                                    <div className="flex justify-between items-center mb-4">
                                                        <h5 className="text-lg font-semibold text-accent-blue">
                                                            Changes in This Version
                                                        </h5>
                                                        <div className="flex space-x-2">
                                                            {snapshotChanges?.added?.length > 0 && (
                                                                <span className="text-xs bg-green-500/10 text-green-500 px-2 py-1 rounded-full">
                                                                    +{snapshotChanges.added.length} Added
                                                                </span>
                                                            )}
                                                            {snapshotChanges?.updated?.length > 0 && (
                                                                <span className="text-xs bg-yellow-500/10 text-yellow-500 px-2 py-1 rounded-full">
                                                                    ~{snapshotChanges.updated.length} Updated
                                                                </span>
                                                            )}
                                                            {snapshotChanges?.deleted?.length > 0 && (
                                                                <span className="text-xs bg-red-500/10 text-red-500 px-2 py-1 rounded-full">
                                                                    -{snapshotChanges.deleted.length} Deleted
                                                                </span>
                                                            )}
                                                        </div>
                                                    </div>
                                                    {loadingSnapshotChanges ? (
                                                        <LoadingSpinner />
                                                    ) : snapshotChanges ? (
                                                        <div className="space-y-6">
                                                            {snapshotChanges.added?.length > 0 && (
                                                                <div className="bg-green-50 dark:bg-green-900/10 rounded-lg overflow-hidden border border-green-100 dark:border-green-900/20">
                                                                    <div className="bg-green-100 dark:bg-green-900/20 px-4 py-2 flex items-center">
                                                                        <div className="w-2 h-2 rounded-full bg-green-500 mr-2"></div>
                                                                        <h6 className="text-sm font-semibold text-green-700 dark:text-green-400">
                                                                            Added Records ({snapshotChanges.added.length})
                                                                        </h6>
                                                                    </div>
                                                                    <div className="overflow-x-auto max-h-64">
                                                                        <table className="w-full">
                                                                            <thead>
                                                                                <tr className="bg-green-50 dark:bg-green-900/10 text-left">
                                                                                    {Object.keys(snapshotChanges.added[0] || {}).map((key) => (
                                                                                        <th
                                                                                            key={key}
                                                                                            className="px-3 py-2 text-xs font-medium text-green-600 dark:text-green-300 uppercase tracking-wider"
                                                                                        >
                                                                                            {key}
                                                                                        </th>
                                                                                    ))}
                                                                                </tr>
                                                                            </thead>
                                                                            <tbody className="divide-y divide-green-100 dark:divide-green-900/20">
                                                                                {snapshotChanges.added.slice(0, 5).map((row, idx) => (
                                                                                    <tr
                                                                                        key={idx}
                                                                                        className="hover:bg-green-50 dark:hover:bg-green-900/20 transition-colors"
                                                                                    >
                                                                                        {Object.entries(row).map(([key, value], i) => (
                                                                                            <td
                                                                                                key={i}
                                                                                                className="px-3 py-2 text-sm text-green-800 dark:text-green-200 whitespace-nowrap"
                                                                                            >
                                                                                                {JSON.stringify(value)}
                                                                                            </td>
                                                                                        ))}
                                                                                    </tr>
                                                                                ))}
                                                                            </tbody>
                                                                        </table>
                                                                        {snapshotChanges.added.length > 5 && (
                                                                            <div className="px-3 py-2 text-xs text-green-600 dark:text-green-300 bg-green-50 dark:bg-green-900/10">
                                                                                Showing 5 of {snapshotChanges.added.length} added records
                                                                            </div>
                                                                        )}
                                                                    </div>
                                                                </div>
                                                            )}
                                                            {snapshotChanges.updated?.length > 0 && (
                                                                <div className="bg-yellow-50 dark:bg-yellow-900/10 rounded-lg overflow-hidden border border-yellow-100 dark:border-yellow-900/20">
                                                                    <div className="bg-yellow-100 dark:bg-yellow-900/20 px-4 py-2 flex items-center">
                                                                        <div className="w-2 h-2 rounded-full bg-yellow-500 mr-2"></div>
                                                                        <h6 className="text-sm font-semibold text-yellow-700 dark:text-yellow-400">
                                                                            Updated Records ({snapshotChanges.updated.length})
                                                                        </h6>
                                                                    </div>
                                                                    <div className="overflow-x-auto max-h-64">
                                                                        <table className="w-full">
                                                                            <thead>
                                                                                <tr className="bg-yellow-50 dark:bg-yellow-900/10 text-left">
                                                                                    <th className="px-3 py-2 text-xs font-medium text-yellow-600 dark:text-yellow-300 uppercase tracking-wider">ID</th>
                                                                                    <th className="px-3 py-2 text-xs font-medium text-yellow-600 dark:text-yellow-300 uppercase tracking-wider">Field</th>
                                                                                    <th className="px-3 py-2 text-xs font-medium text-yellow-600 dark:text-yellow-300 uppercase tracking-wider">Old Value</th>
                                                                                    <th className="px-3 py-2 text-xs font-medium text-yellow-600 dark:text-yellow-300 uppercase tracking-wider">New Value</th>
                                                                                </tr>
                                                                            </thead>
                                                                            <tbody className="divide-y divide-yellow-100 dark:divide-yellow-900/20">
                                                                                {snapshotChanges.updated.flatMap((update) =>
                                                                                    Object.entries(update.changes || {}).map(([field, values], i) => (
                                                                                        <tr
                                                                                            key={`${update.id}-${field}`}
                                                                                            className="hover:bg-yellow-50 dark:hover:bg-yellow-900/20 transition-colors"
                                                                                        >
                                                                                            <td className="px-3 py-2 text-sm text-yellow-800 dark:text-yellow-200 whitespace-nowrap">
                                                                                                {update.id}
                                                                                            </td>
                                                                                            <td className="px-3 py-2 text-sm text-yellow-800 dark:text-yellow-200 whitespace-nowrap">
                                                                                                {field}
                                                                                            </td>
                                                                                            <td className="px-3 py-2 text-sm text-yellow-800 dark:text-yellow-200 whitespace-nowrap line-through text-red-500 dark:text-red-400">
                                                                                                {JSON.stringify(values.old_value)}
                                                                                            </td>
                                                                                            <td className="px-3 py-2 text-sm text-yellow-800 dark:text-yellow-200 whitespace-nowrap text-green-500 dark:text-green-400">
                                                                                                {JSON.stringify(values.new_value)}
                                                                                            </td>
                                                                                        </tr>
                                                                                    ))
                                                                                )}
                                                                            </tbody>
                                                                        </table>
                                                                        {snapshotChanges.updated.length > 5 && (
                                                                            <div className="px-3 py-2 text-xs text-yellow-600 dark:text-yellow-300 bg-yellow-50 dark:bg-yellow-900/10">
                                                                                Showing 5 of {snapshotChanges.updated.length} updated fields
                                                                            </div>
                                                                        )}
                                                                    </div>
                                                                </div>
                                                            )}
                                                            {snapshotChanges.deleted?.length > 0 && (
                                                                <div className="bg-red-50 dark:bg-red-900/10 rounded-lg overflow-hidden border border-red-100 dark:border-red-900/20">
                                                                    <div className="bg-red-100 dark:bg-red-900/20 px-4 py-2 flex items-center">
                                                                        <div className="w-2 h-2 rounded-full bg-red-500 mr-2"></div>
                                                                        <h6 className="text-sm font-semibold text-red-700 dark:text-red-400">
                                                                            Deleted Records ({snapshotChanges.deleted.length})
                                                                        </h6>
                                                                    </div>
                                                                    <div className="overflow-x-auto max-h-64">
                                                                        <table className="w-full">
                                                                            <thead>
                                                                                <tr className="bg-red-50 dark:bg-red-900/10 text-left">
                                                                                    {Object.keys(snapshotChanges.deleted[0] || {}).map((key) => (
                                                                                        <th
                                                                                            key={key}
                                                                                            className="px-3 py-2 text-xs font-medium text-red-600 dark:text-red-300 uppercase tracking-wider"
                                                                                        >
                                                                                            {key}
                                                                                        </th>
                                                                                    ))}
                                                                                </tr>
                                                                            </thead>
                                                                            <tbody className="divide-y divide-red-100 dark:divide-red-900/20">
                                                                                {snapshotChanges.deleted.slice(0, 5).map((row, idx) => (
                                                                                    <tr
                                                                                        key={idx}
                                                                                        className="hover:bg-red-50 dark:hover:bg-red-900/20 transition-colors"
                                                                                    >
                                                                                        {Object.entries(row).map(([key, value], i) => (
                                                                                            <td
                                                                                                key={i}
                                                                                                className="px-3 py-2 text-sm text-red-800 dark:text-red-200 whitespace-nowrap"
                                                                                            >
                                                                                                {JSON.stringify(value)}
                                                                                            </td>
                                                                                        ))}
                                                                                    </tr>
                                                                                ))}
                                                                            </tbody>
                                                                        </table>
                                                                        {snapshotChanges.deleted.length > 5 && (
                                                                            <div className="px-3 py-2 text-xs text-red-600 dark:text-red-300 bg-red-50 dark:bg-red-900/10">
                                                                                Showing 5 of {snapshotChanges.deleted.length} deleted records
                                                                            </div>
                                                                        )}
                                                                    </div>
                                                                </div>
                                                            )}
                                                            {!snapshotChanges.added?.length &&
                                                                !snapshotChanges.updated?.length &&
                                                                !snapshotChanges.deleted?.length && (
                                                                    <div className="text-center py-6">
                                                                        <p className="text-medium-gray dark:text-white/70">
                                                                            No data changes detected in this version
                                                                        </p>
                                                                    </div>
                                                                )}
                                                        </div>
                                                    ) : (
                                                        <div className="text-center py-6">
                                                            {snapshotChanges?.error ? (
                                                                <p className="text-red-500">{snapshotChanges.error}</p>
                                                            ) : (
                                                                <p className="text-medium-gray dark:text-white/70">
                                                                    Change information not available
                                                                </p>
                                                            )}
                                                        </div>
                                                    )}
                                                </div>
                                            </motion.div>
                                        )}
                                    </div>
                                ) : (
                                    <p className="text-medium-gray dark:text-white">No snapshots available</p>
                                )}
                            </div>
                        )}
                        {activeTab === "metrics" && (
                            <div>
                                <h4 className="text-lg font-montserrat font-semibold mb-4 text-medium-gray dark:text-accent-blue">Key Metrics</h4>
                                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                                    <StatCard
                                        title="File Size"
                                        value={selectedItem.details && selectedItem.details.file_size ? formatFileSize(selectedItem.details.file_size) : "N/A"}
                                        color="blue"
                                    />
                                    <StatCard
                                        title="Row Count"
                                        value={selectedItem.details && selectedItem.details.num_rows ? formatNumber(selectedItem.details.num_rows) : "N/A"}
                                        color="green"
                                    />
                                    {selectedItem.details?.partition_keys?.length > 0 && (
                                        <StatCard
                                            title="Partition Keys"
                                            value={selectedItem.details.partition_keys.join(", ")}
                                            color="purple"
                                        />
                                    )}
                                    {selectedItem.details?.partitions?.length > 0 && (
                                        <StatCard
                                            title="Partitions"
                                            value={selectedItem.details.partitions.length}
                                            color="yellow"
                                        />
                                    )}
                                </div>
                            </div>
                        )}
                    </>
                ) : (
                    <>
                        {loadingDataMap[selectedItem.file] ? (
                            <LoadingSpinner />
                        ) : !fileData ? (
                            <p className="text-medium-gray dark:text-white">No data available</p>
                        ) : fileData.error ? (
                            <p className="text-red-500">{fileData.error}</p>
                        ) : (
                            <>
                                {activeTab === "preview" && (
                                    <div className="overflow-x-auto">
                                        <div className="mb-4 flex justify-between items-center">
                                            <h4 className="text-lg font-semibold text-medium-gray dark:text-accent-blue">
                                                Data Preview
                                            </h4>
                                            {fileData?.currentVersion && (
                                                <div className="text-sm bg-accent-blue/10 text-accent-blue px-3 py-1 rounded-full">
                                                    Version: {fileData.currentVersion.version || fileData.currentVersion.snapshot_id || fileData.currentVersion}
                                                </div>
                                            )}
                                        </div>
                                        <table className="w-full border-collapse border border-subtle-gray dark:border-white/20 bg-white dark:bg-dark-teal">
                                            <thead>
                                                <tr className="bg-subtle-gray dark:bg-medium-gray">
                                                    {Object.keys(fileData.data[0] || {})
                                                        .filter((key) => !hiddenColumns[selectedItem.file]?.[key])
                                                        .map((key) => (
                                                            <th
                                                                key={key}
                                                                className="p-2 whitespace-nowrap text-medium-gray dark:text-white text-left border"
                                                            >
                                                                {key}
                                                            </th>
                                                        ))}
                                                </tr>
                                            </thead>
                                            <tbody>
                                                {fileData.data.map((row, idx) => (
                                                    <tr key={idx} className="hover:bg-accent-blue/10 dark:hover:bg-white/10 transition-all duration-200">
                                                        {Object.keys(row)
                                                            .filter((key) => !hiddenColumns[selectedItem.file]?.[key])
                                                            .map((key, i) => (
                                                                <td
                                                                    key={i}
                                                                    className="p-2 whitespace-nowrap text-medium-gray dark:text-white border"
                                                                >
                                                                    {row[key]}
                                                                </td>
                                                            ))}
                                                    </tr>
                                                ))}
                                            </tbody>
                                        </table>
                                    </div>
                                )}
                                {activeTab === "columns" && (
                                    <div>
                                        <h4 className="text-lg font-montserrat font-semibold mb-4 text-medium-gray dark:text-accent-blue">Manage Columns</h4>
                                        <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 gap-3">
                                            {Object.keys(fileData.data[0] || {}).map((key) => (
                                                <div key={key} className="flex items-center p-2 border rounded-md">
                                                    <input
                                                        type="checkbox"
                                                        id={`col-${key}`}
                                                        checked={!hiddenColumns[selectedItem.file]?.[key]}
                                                        onChange={() => toggleHideColumn(selectedItem.file, key)}
                                                        className="mr-2"
                                                    />
                                                    <label htmlFor={`col-${key}`} className="text-sm text-medium-gray dark:text-white mr-2 flex-grow">{key}</label>
                                                    <button
                                                        onClick={() => togglePinColumn(selectedItem.file, key)}
                                                        className={`text-xs px-2 py-1 rounded ${pinnedColumns[selectedItem.file]?.[key]
                                                            ? "bg-accent-blue text-white"
                                                            : "bg-subtle-gray text-medium-gray dark:bg-medium-gray dark:text-white"
                                                            } hover:scale-105 transition-all duration-300`}
                                                    >
                                                        {pinnedColumns[selectedItem.file]?.[key] ? "Unpin" : "Pin"}
                                                    </button>
                                                </div>
                                            ))}
                                        </div>
                                    </div>
                                )}
                                {activeTab === "stats" && (
                                    <div>
                                        <h4 className="text-lg font-montserrat font-semibold mb-4 text-medium-gray dark:text-accent-blue">Column Statistics</h4>
                                        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
                                            {Object.keys(fileData.data[0] || {}).map((col, idx) => {
                                                const stats = computeColumnStats(fileData.data, col);
                                                return (
                                                    <motion.div
                                                        key={idx}
                                                        className={`p-4 rounded-xl shadow-lg dark:bg-dark-teal/80 backdrop-blur-md border border-white/10 ${darkMode
                                                            ? 'hover:shadow-[0_0_15px_rgba(0,161,214,0.5)]'
                                                            : 'hover:shadow-[0_4px_12px_rgba(0,0,0,0.15)]'
                                                            } transition-shadow duration-300`}
                                                        initial={{ opacity: 0, y: 20 }}
                                                        animate={{ opacity: 1, y: 0 }}
                                                        transition={{ duration: 0.3, delay: idx * 0.05 }}
                                                    >
                                                        <h5 className="text-md font-montserrat font-medium mb-2 text-accent-blue">{col}</h5>
                                                        {stats.type === "numeric" ? (
                                                            <div className="text-sm space-y-2 text-black dark:text-white">
                                                                <p><span className="font-semibold text-accent-blue/80">Min:</span> {stats.min !== null ? stats.min.toFixed(2) : "N/A"}</p>
                                                                <p><span className="font-semibold text-accent-blue/80">Max:</span> {stats.max !== null ? stats.max.toFixed(2) : "N/A"}</p>
                                                                <p><span className="font-semibold text-accent-blue/80">Mean:</span> {stats.mean !== null ? stats.mean.toFixed(2) : "N/A"}</p>
                                                                <p><span className="font-semibold text-accent-blue/80">Std Dev:</span> {stats.std !== null ? stats.std.toFixed(2) : "N/A"}</p>
                                                            </div>
                                                        ) : (
                                                            <div className="text-sm text-black dark:text-white">
                                                                <p className="font-semibold text-accent-blue/80 mb-2">Top 5 Values:</p>
                                                                <ul className="space-y-1">
                                                                    {stats.topValues.map(([value, count], i) => (
                                                                        <li key={i} className="flex justify-between">
                                                                            <span>{value}</span>
                                                                            <span className="text-accent-blue/80">{count}</span>
                                                                        </li>
                                                                    ))}
                                                                </ul>
                                                            </div>
                                                        )}
                                                    </motion.div>
                                                );
                                            })}
                                        </div>
                                    </div>
                                )}
                                {activeTab === "visualization" && (
                                    <div>
                                        <h4 className="text-lg font-montserrat font-semibold mb-4 text-medium-gray dark:text-accent-blue">Data Distribution</h4>
                                        <select
                                            className="mb-4 p-2 border rounded-md w-full max-w-md bg-white dark:bg-dark-teal border-subtle-gray dark:border-white/20 text-medium-gray dark:text-white"
                                            value={selectedChartColumn[selectedItem.file] || ""}
                                            onChange={(e) => setSelectedChartColumn((prev) => ({ ...prev, [selectedItem.file]: e.target.value }))}
                                        >
                                            <option value="">Select a column to visualize</option>
                                            {Object.keys(fileData.data[0] || {}).map((col) => (
                                                <option key={col} value={col}>{col}</option>
                                            ))}
                                        </select>
                                        {selectedChartColumn[selectedItem.file] && (
                                            <div className="bg-white dark:bg-dark-teal/50 rounded-lg p-4 mt-4 overflow-x-auto">
                                                {(() => {
                                                    const stats = computeColumnStats(fileData.data, selectedChartColumn[selectedItem.file]);
                                                    if (stats.type === "numeric") {
                                                        const histogramData = generateHistogramData(fileData.data, selectedChartColumn[selectedItem.file]);
                                                        return histogramData ? (
                                                            <div className="h-64">
                                                                <Bar
                                                                    data={histogramData}
                                                                    options={{
                                                                        responsive: true,
                                                                        maintainAspectRatio: false,
                                                                        plugins: {
                                                                            legend: { position: "top", labels: { color: darkMode ? "white" : "black" } },
                                                                            title: { display: true, text: `Distribution of ${selectedChartColumn[selectedItem.file]}`, color: darkMode ? "white" : "black" },
                                                                        },
                                                                        scales: {
                                                                            x: { ticks: { color: darkMode ? "white" : "black" }, grid: { color: darkMode ? "rgba(255, 255, 255, 0.1)" : "rgba(0, 0, 0, 0.1)" } },
                                                                            y: { ticks: { color: darkMode ? "white" : "black" }, grid: { color: darkMode ? "rgba(255, 255, 255, 0.1)" : "rgba(0, 0, 0, 0.1)" } },
                                                                        },
                                                                    }}
                                                                />
                                                            </div>
                                                        ) : (
                                                            <p>No numeric data to visualize.</p>
                                                        );
                                                    } else {
                                                        const barData = generateCategoricalBarData(stats.topValues, selectedChartColumn[selectedItem.file]);
                                                        return (
                                                            <div className="h-64">
                                                                <Bar
                                                                    data={barData}
                                                                    options={{
                                                                        responsive: true,
                                                                        maintainAspectRatio: false,
                                                                        plugins: {
                                                                            legend: { position: "top", labels: { color: darkMode ? "white" : "black" } },
                                                                            title: { display: true, text: `Top Values in ${selectedChartColumn[selectedItem.file]}`, color: darkMode ? "white" : "black" },
                                                                        },
                                                                        scales: {
                                                                            x: { ticks: { color: darkMode ? "white" : "black" }, grid: { color: darkMode ? "rgba(255, 255, 255, 0.1)" : "rgba(0, 0, 0, 0.1)" } },
                                                                            y: { ticks: { color: darkMode ? "white" : "black" }, grid: { color: darkMode ? "rgba(255, 255, 255, 0.1)" : "rgba(0, 0, 0, 0.1)" } },
                                                                        },
                                                                    }}
                                                                />
                                                            </div>
                                                        );
                                                    }
                                                })()}
                                            </div>
                                        )}
                                    </div>
                                )}
                            </>
                        )}
                    </>
                )}
            </div>
        );
    };

    return (
        <ErrorBoundary>
            <div className="container mx-auto py-10 px-6 max-w-7xl">
                <div className="mb-10">
                    <motion.h1
                        initial={{ opacity: 0, y: -20 }}
                        animate={{ opacity: 1, y: 0 }}
                        transition={{ duration: 0.7 }}
                        className="text-4xl font-montserrat font-bold bg-gradient-to-r from-accent-blue via-blue-500 to-purple-600 text-transparent bg-clip-text mb-2"
                    >
                        Data Lake Explorer
                    </motion.h1>
                    <motion.p
                        initial={{ opacity: 0 }}
                        animate={{ opacity: 0.7 }}
                        transition={{ duration: 0.7, delay: 0.2 }}
                        className="text-lg text-medium-gray/80 dark:text-white/70 mb-8"
                    >
                        Explore, analyze and visualize your data lake with ease
                    </motion.p>
                    <form onSubmit={handleSubmit} className="flex flex-col md:flex-row gap-4 mb-10">
                        <motion.div
                            initial={{ opacity: 0, x: -20 }}
                            animate={{ opacity: 1, x: 0 }}
                            transition={{ duration: 0.5 }}
                            className="flex-grow relative"
                        >
                            <input
                                type="text"
                                value={objectStorePath}
                                onChange={(e) => setObjectStorePath(e.target.value)}
                                placeholder="s3://bucket-name/prefix"
                                className="w-full p-5 rounded-2xl border border-subtle-gray/20 dark:border-white/10 bg-white/90 dark:bg-dark-teal/90 text-medium-gray dark:text-white focus:outline-none focus:ring-2 focus:ring-accent-blue/70 transition-all duration-300 shadow-lg"
                            />
                            <div className="absolute inset-y-0 left-0 flex items-center pl-4">
                                <span className="text-accent-blue/70">
                                    <svg xmlns="http://www.w3.org/2000/svg" className="h-6 w-6" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 12a9 9 0 01-9 9m9-9a9 9 0 00-9-9m9 9H3m9 9a9 9 0 01-9-9m9 9c1.657 0 3-4.03 3-9s-1.343-9-3-9m0 18c-1.657 0-3-4.03-3-9s1.343-9 3-9m-9 9a9 9 0 019-9" />
                                    </svg>
                                </span>
                            </div>
                            <div className="absolute inset-y-0 right-0 flex items-center pr-4">
                                <span className="text-medium-gray/50 dark:text-white/50 text-sm bg-gray-100 dark:bg-gray-700 px-2 py-1 rounded-md">Press Enter</span>
                            </div>
                        </motion.div>
                        <motion.button
                            initial={{ opacity: 0, scale: 0.9 }}
                            animate={{ opacity: 1, scale: 1 }}
                            transition={{ duration: 0.5 }}
                            whileHover={{ scale: 1.05 }}
                            whileTap={{ scale: 0.95 }}
                            type="submit"
                            className="px-8 py-5 bg-gradient-to-r from-accent-blue to-blue-600 text-white font-medium rounded-2xl shadow-lg hover:shadow-accent-blue/20 hover:shadow-xl focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-opacity-50 transition-all duration-300"
                        >
                            Explore Data
                        </motion.button>
                    </form>
                    {metadata.length > 0 && (
                        <motion.div
                            initial={{ opacity: 0, y: 20 }}
                            animate={{ opacity: 1, y: 0 }}
                            transition={{ duration: 0.7, delay: 0.3 }}
                            className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-6 mb-10"
                        >
                            <StatCard
                                title="Total Files"
                                value={metadata.length}
                                color="blue"
                            />
                            <StatCard
                                title="Total Size"
                                value={formatFileSize(
                                    metadata.reduce((sum, item) => sum + (item.details?.file_size || 0), 0)
                                )}
                                color="green"
                            />
                            <StatCard
                                title="Avg File Size"
                                value={formatFileSize(
                                    metadata.length > 0
                                        ? metadata.reduce((sum, item) => sum + (item.details?.file_size || 0), 0) / metadata.length
                                        : 0
                                )}
                                color="purple"
                            />
                            <StatCard
                                title="Table Formats"
                                value={new Set(metadata.filter(item => item.details?.format).map(item => item.details.format)).size}
                                color="yellow"
                            />
                        </motion.div>
                    )}
                    {metadata.length > 0 && (
                        <motion.div
                            initial={{ opacity: 0, y: 20 }}
                            animate={{ opacity: 1, y: 0 }}
                            transition={{ duration: 0.7, delay: 0.4 }}
                            className="mb-10"
                        >
                            <div className="relative">
                                <input
                                    type="text"
                                    placeholder="Search files..."
                                    onChange={(e) => handleSearch(e.target.value)}
                                    className="w-full p-5 rounded-2xl border border-subtle-gray/20 dark:border-white/10 bg-white/90 dark:bg-dark-teal/90 text-medium-gray dark:text-white focus:outline-none focus:ring-2 focus:ring-accent-blue/70 transition-all duration-300 shadow-lg pl-14"
                                />
                                <div className="absolute inset-y-0 left-0 flex items-center pl-5">
                                    <svg xmlns="http://www.w3.org/2000/svg" className="h-6 w-6 text-accent-blue/70" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
                                    </svg>
                                </div>
                                <div className="absolute inset-y-0 right-0 flex items-center pr-5">
                                    <span className="text-medium-gray/50 dark:text-white/50 text-sm bg-gray-100 dark:bg-gray-700 px-2 py-1 rounded-md">ctrl+K</span>
                                </div>
                            </div>
                        </motion.div>
                    )}
                </div>
                {loading ? (
                    <LoadingSpinner />
                ) : error ? (
                    <motion.div
                        initial={{ opacity: 0 }}
                        animate={{ opacity: 1 }}
                        className="bg-red-50 dark:bg-red-900/20 p-8 rounded-2xl border border-red-200 dark:border-red-500/30 shadow-lg"
                    >
                        <p className="text-red-500 flex items-center">
                            <svg xmlns="http://www.w3.org/2000/svg" className="h-6 w-6 mr-2" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
                            </svg>
                            {error}
                        </p>
                    </motion.div>
                ) : (
                    <div>
                        {paginatedMetadata.length > 0 ? (
                            <motion.div
                                initial={{ opacity: 0, y: 20 }}
                                animate={{ opacity: 1, y: 0 }}
                                transition={{ duration: 0.5 }}
                                className="overflow-hidden rounded-2xl bg-white/90 dark:bg-dark-teal/90 shadow-2xl border border-white/20 dark:border-white/5"
                            >
                                <div className="overflow-x-auto">
                                    <table className="w-full whitespace-nowrap">
                                        <thead>
                                            <tr className="bg-gradient-to-r from-accent-blue/10 to-blue-500/5 dark:from-accent-blue/20 dark:to-blue-500/10 text-medium-gray dark:text-white text-left">
                                                <th className="px-8 py-5 font-semibold">Name</th>
                                                <th className="px-8 py-5 font-semibold">Format</th>
                                                <th className="px-8 py-5 font-semibold">Size</th>
                                                <th className="px-8 py-5 font-semibold">Rows</th>
                                                <th className="px-8 py-5 font-semibold">Columns</th>
                                                <th className="px-8 py-5 font-semibold">Actions</th>
                                            </tr>
                                        </thead>
                                        <tbody>
                                            {paginatedMetadata.map((item, idx) => (
                                                <motion.tr
                                                    key={idx}
                                                    initial={{ opacity: 0, y: 10 }}
                                                    animate={{ opacity: 1, y: 0 }}
                                                    transition={{ duration: 0.3, delay: idx * 0.05 }}
                                                    className="border-b border-subtle-gray/10 dark:border-white/5 hover:bg-accent-blue/5 dark:hover:bg-white/5 transition-all duration-300"
                                                >
                                                    <td className="px-8 py-5">
                                                        <div className="flex items-center">
                                                            <span className="mr-4 bg-white/90 dark:bg-dark-teal/90 p-2 rounded-lg shadow-sm">{getFileIcon(item.extension, item.details?.format)}</span>
                                                            <span className="text-medium-gray dark:text-white font-medium truncate max-w-xs">
                                                                {item.filename}
                                                            </span>
                                                        </div>
                                                    </td>
                                                    <td className="px-8 py-5 text-medium-gray dark:text-white">
                                                        <span className="px-3 py-1 bg-accent-blue/10 text-accent-blue rounded-full text-sm">
                                                            {item.displayFormat}
                                                        </span>
                                                    </td>
                                                    <td className="px-8 py-5 text-medium-gray dark:text-white">
                                                        {item.details && item.details.file_size ? formatFileSize(item.details.file_size) : "N/A"}
                                                    </td>
                                                    <td className="px-8 py-5 text-medium-gray dark:text-white">
                                                        {item.details && item.details.num_rows ? formatNumber(item.details.num_rows) : "N/A"}
                                                    </td>
                                                    <td className="px-8 py-5 text-medium-gray dark:text-white">
                                                        {item.details && Array.isArray(item.details.columns) ? (
                                                            <span className="inline-flex items-center">
                                                                <span className="bg-green-100 dark:bg-green-900/20 text-green-800 dark:text-green-300 text-xs font-medium px-2.5 py-1 rounded-full">
                                                                    {item.details.columns.length}
                                                                </span>
                                                            </span>
                                                        ) : "N/A"}
                                                    </td>
                                                    <td className="px-8 py-5">
                                                        <div className="flex gap-3">
                                                            <motion.button
                                                                whileHover={{ scale: 1.05 }}
                                                                whileTap={{ scale: 0.95 }}
                                                                onClick={() => openDetailsModal(item)}
                                                                className="px-5 py-2.5 bg-gradient-to-r from-accent-blue/20 to-blue-500/10 text-accent-blue rounded-lg hover:shadow-md transition-all duration-300"
                                                            >
                                                                View Details
                                                            </motion.button>
                                                            <motion.button
                                                                whileHover={{ scale: 1.05 }}
                                                                whileTap={{ scale: 0.95 }}
                                                                onClick={() => openDataModal(item)}
                                                                className="px-5 py-2.5 bg-gradient-to-r from-green-500/20 to-green-600/10 text-green-600 dark:text-green-400 rounded-lg hover:shadow-md transition-all duration-300"
                                                            >
                                                                View Data
                                                            </motion.button>
                                                        </div>
                                                    </td>
                                                </motion.tr>
                                            ))}
                                        </tbody>
                                    </table>
                                </div>
                                <div className="flex justify-between items-center px-8 py-6 bg-white/90 dark:bg-dark-teal/90 border-t border-subtle-gray/10 dark:border-white/5">
                                    <div className="text-sm text-medium-gray dark:text-white">
                                        Showing <span className="font-semibold text-accent-blue">{(currentPage - 1) * itemsPerPage + 1}</span> to{" "}
                                        <span className="font-semibold text-accent-blue">{Math.min(currentPage * itemsPerPage, filteredMetadata.length)}</span> of{" "}
                                        <span className="font-semibold text-accent-blue">{filteredMetadata.length}</span> files
                                    </div>
                                    <div className="flex gap-3">
                                        <motion.button
                                            whileHover={{ scale: 1.05 }}
                                            whileTap={{ scale: 0.95 }}
                                            onClick={() => setCurrentPage((prev) => Math.max(prev - 1, 1))}
                                            disabled={currentPage === 1}
                                            className={`px-5 py-2.5 rounded-lg transition-all duration-300 flex items-center ${currentPage === 1
                                                ? "bg-subtle-gray/30 dark:bg-medium-gray/30 text-medium-gray/50 dark:text-white/50 cursor-not-allowed"
                                                : "bg-gradient-to-r from-accent-blue/20 to-blue-500/10 text-accent-blue hover:shadow-md"
                                                }`}
                                        >
                                            <svg xmlns="http://www.w3.org/2000/svg" className="h-4 w-4 mr-1" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                                                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 19l-7-7 7-7" />
                                            </svg>
                                            Previous
                                        </motion.button>
                                        <span className="px-5 py-2.5 bg-subtle-gray/30 dark:bg-medium-gray/30 text-medium-gray dark:text-white rounded-lg font-semibold">
                                            {currentPage}
                                        </span>
                                        <motion.button
                                            whileHover={{ scale: 1.05 }}
                                            whileTap={{ scale: 0.95 }}
                                            onClick={() =>
                                                setCurrentPage((prev) =>
                                                    Math.min(prev + 1, Math.ceil(filteredMetadata.length / itemsPerPage))
                                                )
                                            }
                                            disabled={currentPage >= Math.ceil(filteredMetadata.length / itemsPerPage)}
                                            className={`px-5 py-2.5 rounded-lg transition-all duration-300 flex items-center ${currentPage >= Math.ceil(filteredMetadata.length / itemsPerPage)
                                                ? "bg-subtle-gray/30 dark:bg-medium-gray/30 text-medium-gray/50 dark:text-white/50 cursor-not-allowed"
                                                : "bg-gradient-to-r from-accent-blue/20 to-blue-500/10 text-accent-blue hover:shadow-md"
                                                }`}
                                        >
                                            Next
                                            <svg xmlns="http://www.w3.org/2000/svg" className="h-4 w-4 ml-1" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                                                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
                                            </svg>
                                        </motion.button>
                                    </div>
                                </div>
                            </motion.div>
                        ) : (
                            <motion.div
                                initial={{ opacity: 0 }}
                                animate={{ opacity: 1 }}
                                transition={{ duration: 0.5 }}
                                className="text-center p-16 bg-white/50 dark:bg-dark-teal/50 rounded-2xl border border-subtle-gray/20 dark:border-white/10 shadow-xl backdrop-blur-sm"
                            >
                                <div className="flex flex-col items-center justify-center space-y-4">
                                    <svg xmlns="http://www.w3.org/2000/svg" className="h-16 w-16 text-accent-blue/50" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M9 13h6m-3-3v6m5 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
                                    </svg>
                                    <p className="text-medium-gray dark:text-white/70 text-xl font-medium">
                                        {metadata.length === 0 && objectStorePath
                                            ? "No files found in this location."
                                            : "Enter a path and press Explore Data to discover your files."}
                                    </p>
                                    {metadata.length === 0 && objectStorePath && (
                                        <p className="text-medium-gray/70 dark:text-white/50 max-w-md">
                                            Try a different path or check your connection. Make sure you have the correct permissions.
                                        </p>
                                    )}
                                </div>
                            </motion.div>
                        )}
                    </div>
                )}
                <Modal
                    isOpen={detailsModalOpen}
                    onClose={() => setDetailsModalOpen(false)}
                    title={`File Details: ${selectedItem?.filename || ""}`}
                    size="xl"
                >
                    {renderModalContent("details")}
                </Modal>
                <Modal
                    isOpen={dataModalOpen}
                    onClose={() => setDataModalOpen(false)}
                    title={`Data Preview: ${selectedItem?.filename || ""}`}
                    size="xl"
                >
                    {renderModalContent("data")}
                </Modal>
            </div>
        </ErrorBoundary>
    );
};

export default MetadataViewer;