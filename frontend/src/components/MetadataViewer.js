import React, { useState, useEffect, useCallback, useMemo } from "react";
import axios from "axios";
import { FaFileAlt, FaFileExcel, FaFilePdf, FaFileImage, FaFileCode, FaTimes } from "react-icons/fa";
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
import { motion, AnimatePresence } from "framer-motion";

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

// File icon handling
const getFileIcon = (extension) => {
    const ext = extension.toLowerCase();
    if (["xls", "xlsx", "csv"].includes(ext)) return <FaFileExcel className="text-green-500" />;
    if (["pdf"].includes(ext)) return <FaFilePdf className="text-red-500" />;
    if (["jpg", "jpeg", "png", "gif"].includes(ext)) return <FaFileImage className="text-yellow-500" />;
    if (["parquet", "delta", "iceberg", "hudi"].includes(ext)) return <FaFileCode className="text-accent-blue" />;
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
        <div className="fixed inset-0 z-50 flex items-center justify-center overflow-auto bg-black bg-opacity-50 p-4">
            <div className={`bg-white dark:bg-dark-teal rounded-lg shadow-xl ${sizeClasses[size]} w-full max-h-[90vh] flex flex-col`}>
                <div className="flex justify-between items-center p-4 border-b border-subtle-gray dark:border-white/20">
                    <h3 className="text-lg font-montserrat font-semibold text-medium-gray dark:text-accent-blue">{title}</h3>
                    <button
                        onClick={onClose}
                        className="p-1 rounded-full hover:bg-gray-200 dark:hover:bg-gray-700 transition-colors"
                    >
                        <FaTimes className="text-medium-gray dark:text-white" />
                    </button>
                </div>
                <div className="flex-1 overflow-auto p-4">
                    {children}
                </div>
            </div>
        </div>
    );
};

const Tabs = ({ tabs, activeTab, setActiveTab }) => {
    return (
        <div className="mb-6">
            <div className="flex border-b border-subtle-gray dark:border-white/20">
                {tabs.map((tab) => (
                    <button
                        key={tab.id}
                        className={`px-4 py-2 font-montserrat font-medium transition-all duration-200 ${activeTab === tab.id
                            ? "text-accent-blue border-b-2 border-accent-blue"
                            : "text-medium-gray dark:text-white hover:text-accent-blue"
                            }`}
                        onClick={() => setActiveTab(tab.id)}
                    >
                        {tab.label}
                    </button>
                ))}
            </div>
        </div>
    );
};

const LoadingSpinner = () => (
    <div className="flex justify-center items-center h-32">
        <div className="animate-spin rounded-full h-12 w-12 border-t-2 border-b-2 border-accent-blue dark:border-white"></div>
    </div>
);

const StatCard = ({ title, value, icon, color = "blue" }) => {
    const colors = {
        blue: "bg-blue-500/10 text-blue-500 border-blue-300",
        green: "bg-green-500/10 text-green-500 border-green-300",
        red: "bg-red-500/10 text-red-500 border-red-300",
        yellow: "bg-yellow-500/10 text-yellow-500 border-yellow-300",
        purple: "bg-purple-500/10 text-purple-500 border-purple-300",
    };

    return (
        <div className={`p-4 rounded-lg shadow ${colors[color]} border`}>
            <div className="flex items-center justify-between">
                <div>
                    <p className="text-sm opacity-80">{title}</p>
                    <p className="text-xl font-semibold mt-1">{value}</p>
                </div>
                {icon && <div className="text-2xl opacity-80">{icon}</div>}
            </div>
        </div>
    );
};

const MetadataViewer = ({ darkMode }) => {
    const [objectStorePath, setObjectStorePath] = useState("s3://test-bucket");
    const [metadata, setMetadata] = useState([]);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState(null);
    const [searchQuery, setSearchQuery] = useState("");
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

    const itemsPerPage = 10;

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
        setSelectedPartition(partition);
        setLoadingPartitionData(true);
        fetchData(selectedItem, null, partition);
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
                const extension = parts.length > 1 ? parts[parts.length - 1] : (item.details && item.details.format ? item.details.format : "");
                return { ...item, filename, extension };
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
                                        <div className="flex flex-wrap gap-2">
                                            {selectedItem.details.partitions.map((partition, i) => (
                                                <motion.button
                                                    key={i}
                                                    whileHover={{ scale: 1.05 }}
                                                    whileTap={{ scale: 0.95 }}
                                                    className={`p-3 rounded-md border border-subtle-gray dark:border-white/20 bg-white/50 dark:bg-dark-teal/50 cursor-pointer ${selectedPartition === partition ? "bg-accent-blue/20 border-accent-blue" : ""}`}
                                                    onClick={() => handlePartitionClick(partition)}
                                                >
                                                    <div className="font-semibold text-accent-blue">{partition}</div>
                                                </motion.button>
                                            ))}
                                        </div>
                                        <AnimatePresence>
                                            {selectedPartition && (
                                                <motion.div
                                                    initial={{ opacity: 0, y: 20, scale: 0.95 }}
                                                    animate={{ opacity: 1, y: 0, scale: 1 }}
                                                    exit={{ opacity: 0, y: -20, scale: 0.95 }}
                                                    transition={{ duration: 0.5, ease: "easeInOut" }}
                                                    className="mt-4 p-4 rounded-lg bg-gradient-to-r from-accent-blue/10 to-purple-500/10 dark:from-dark-teal/50 dark:to-medium-gray/50 border border-accent-blue/30 shadow-lg"
                                                >
                                                    <h5 className="text-md font-montserrat font-semibold mb-3 text-accent-blue">
                                                        Data for Partition: {selectedPartition}
                                                    </h5>
                                                    {loadingPartitionData ? (
                                                        <LoadingSpinner />
                                                    ) : partitionData && partitionData.data ? (
                                                        <div className="overflow-x-auto">
                                                            <table className="w-full border-collapse border border-subtle-gray dark:border-white/20 bg-white dark:bg-dark-teal">
                                                                <thead>
                                                                    <tr className="bg-subtle-gray dark:bg-medium-gray">
                                                                        {Object.keys(partitionData.data[0] || {}).map((key) => (
                                                                            <th key={key} className="p-2 whitespace-nowrap text-medium-gray dark:text-white text-left border">
                                                                                {key}
                                                                            </th>
                                                                        ))}
                                                                    </tr>
                                                                </thead>
                                                                <tbody>
                                                                    {partitionData.data.map((row, idx) => (
                                                                        <tr key={idx} className="hover:bg-accent-blue/10 dark:hover:bg-white/10 transition-all duration-200">
                                                                            {Object.values(row).map((value, i) => (
                                                                                <td key={i} className="p-2 whitespace-nowrap text-medium-gray dark:text-white border">
                                                                                    {value}
                                                                                </td>
                                                                            ))}
                                                                        </tr>
                                                                    ))}
                                                                </tbody>
                                                            </table>
                                                        </div>
                                                    ) : (
                                                        <p className="text-medium-gray dark:text-white">No data available for this partition.</p>
                                                    )}
                                                </motion.div>
                                            )}
                                        </AnimatePresence>
                                    </div>
                                ) : (
                                    <p className="text-medium-gray dark:text-white">No partitions</p>
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
                                                        {snap.is_current && (
                                                            <span className="text-xs bg-green-500/10 text-green-500 px-2 py-1 rounded-full">
                                                                Current
                                                            </span>
                                                        )}
                                                    </div>
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
                                                                        ? `${selectedSnapshot.timestamp.slice(0, 4)}-${selectedSnapshot.timestamp.slice(4, 6)}-${selectedSnapshot.timestamp.slice(6, 8)} ${selectedSnapshot.timestamp.slice(8, 10)}:${selectedSnapshot.timestamp.slice(10, 12)}:${selectedSnapshot.timestamp.slice(12, 14)}`
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

                                                {/* Changes Section */}
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
                                                            {/* Added Records */}
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

                                                            {/* Updated Records */}
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

                                                            {/* Deleted Records */}
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
                                                        className="p-4 rounded-xl shadow-lg bg-dark-teal/80 backdrop-blur-md border border-white/10"
                                                        initial={{ opacity: 0, y: 20 }}
                                                        animate={{ opacity: 1, y: 0 }}
                                                        transition={{ duration: 0.3, delay: idx * 0.05 }}
                                                    >
                                                        <h5 className="text-md font-montserrat font-medium mb-2 text-accent-blue">{col}</h5>
                                                        {stats.type === "numeric" ? (
                                                            <div className="text-sm space-y-2 text-white">
                                                                <p><span className="font-semibold text-accent-blue/80">Min:</span> {stats.min !== null ? stats.min.toFixed(2) : "N/A"}</p>
                                                                <p><span className="font-semibold text-accent-blue/80">Max:</span> {stats.max !== null ? stats.max.toFixed(2) : "N/A"}</p>
                                                                <p><span className="font-semibold text-accent-blue/80">Mean:</span> {stats.mean !== null ? stats.mean.toFixed(2) : "N/A"}</p>
                                                                <p><span className="font-semibold text-accent-blue/80">Std Dev:</span> {stats.std !== null ? stats.std.toFixed(2) : "N/A"}</p>
                                                            </div>
                                                        ) : (
                                                            <div className="text-sm text-white">
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
            <div className="container mx-auto py-6 px-4 max-w-6xl">
                <div className="mb-6">
                    <h1 className="text-2xl font-montserrat font-bold text-medium-gray dark:text-white mb-6">
                        Data Lake Explorer
                    </h1>

                    <form onSubmit={handleSubmit} className="flex flex-col md:flex-row gap-4 mb-6">
                        <input
                            type="text"
                            value={objectStorePath}
                            onChange={(e) => setObjectStorePath(e.target.value)}
                            placeholder="s3://bucket-name/prefix"
                            className="flex-grow p-3 rounded-lg border border-subtle-gray dark:border-white/20 bg-white dark:bg-dark-teal text-medium-gray dark:text-white focus:outline-none focus:ring-2 focus:ring-accent-blue transition-all duration-300"
                        />
                        <button
                            type="submit"
                            className="px-6 py-3 bg-accent-blue text-white font-medium rounded-lg shadow-md hover:bg-blue-600 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-opacity-50 transition-all duration-300"
                        >
                            Browse
                        </button>
                    </form>

                    {metadata.length > 0 && (
                        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4 mb-6">
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
                                title="File Types"
                                value={new Set(metadata.map(item => {
                                    const parts = item.file.split('.');
                                    return parts.length > 1 ? parts[parts.length - 1] : 'unknown';
                                })).size}
                                color="yellow"
                            />
                        </div>
                    )}

                    {metadata.length > 0 && (
                        <div className="mb-6">
                            <input
                                type="text"
                                placeholder="Search files..."
                                onChange={(e) => handleSearch(e.target.value)}
                                className="w-full p-3 rounded-lg border border-subtle-gray dark:border-white/20 bg-white dark:bg-dark-teal text-medium-gray dark:text-white focus:outline-none focus:ring-2 focus:ring-accent-blue transition-all duration-300"
                            />
                        </div>
                    )}
                </div>

                {loading ? (
                    <LoadingSpinner />
                ) : error ? (
                    <div className="text-red-500 p-4 rounded-lg bg-red-100 dark:bg-red-900/20 border border-red-200 dark:border-red-500/30">
                        {error}
                    </div>
                ) : (
                    <div>
                        {paginatedMetadata.length > 0 ? (
                            <div className="overflow-hidden rounded-xl bg-white dark:bg-dark-teal shadow-md">
                                <div className="overflow-x-auto">
                                    <table className="w-full whitespace-nowrap">
                                        <thead>
                                            <tr className="bg-subtle-gray dark:bg-medium-gray/30 text-medium-gray dark:text-white text-left">
                                                <th className="px-6 py-3">Name</th>
                                                <th className="px-6 py-3">Format</th>
                                                <th className="px-6 py-3">Size</th>
                                                <th className="px-6 py-3">Rows</th>
                                                <th className="px-6 py-3">Columns</th>
                                                <th className="px-6 py-3">Actions</th>
                                            </tr>
                                        </thead>
                                        <tbody>
                                            {paginatedMetadata.map((item, idx) => (
                                                <motion.tr
                                                    key={idx}
                                                    initial={{ opacity: 0, y: 10 }}
                                                    animate={{ opacity: 1, y: 0 }}
                                                    transition={{ duration: 0.3, delay: idx * 0.05 }}
                                                    className="border-b border-subtle-gray dark:border-white/10 hover:bg-accent-blue/5 dark:hover:bg-white/5 transition-colors"
                                                >
                                                    <td className="px-6 py-4">
                                                        <div className="flex items-center">
                                                            <span className="mr-2">{getFileIcon(item.extension)}</span>
                                                            <span className="text-medium-gray dark:text-white font-medium truncate max-w-xs">
                                                                {item.filename}
                                                            </span>
                                                        </div>
                                                    </td>
                                                    <td className="px-6 py-4 text-medium-gray dark:text-white">
                                                        {item.extension || (item.details && item.details.format ? item.details.format : "N/A")}
                                                    </td>
                                                    <td className="px-6 py-4 text-medium-gray dark:text-white">
                                                        {item.details && item.details.file_size ? formatFileSize(item.details.file_size) : "N/A"}
                                                    </td>
                                                    <td className="px-6 py-4 text-medium-gray dark:text-white">
                                                        {item.details && item.details.num_rows ? formatNumber(item.details.num_rows) : "N/A"}
                                                    </td>
                                                    <td className="px-6 py-4 text-medium-gray dark:text-white">
                                                        {item.details && Array.isArray(item.details.columns) ? item.details.columns.length : "N/A"}
                                                    </td>
                                                    <td className="px-6 py-4">
                                                        <div className="flex gap-2">
                                                            <button
                                                                onClick={() => openDetailsModal(item)}
                                                                className="px-3 py-1.5 bg-accent-blue/10 text-accent-blue rounded-md hover:bg-accent-blue/20 transition-colors duration-300"
                                                            >
                                                                View Details
                                                            </button>
                                                            <button
                                                                onClick={() => openDataModal(item)}
                                                                className="px-3 py-1.5 bg-green-500/10 text-green-600 dark:text-green-400 rounded-md hover:bg-green-500/20 transition-colors duration-300"
                                                            >
                                                                View Data
                                                            </button>
                                                        </div>
                                                    </td>
                                                </motion.tr>
                                            ))}
                                        </tbody>
                                    </table>
                                </div>

                                <div className="flex justify-between items-center px-6 py-4 bg-white dark:bg-dark-teal border-t border-subtle-gray dark:border-white/10">
                                    <div className="text-sm text-medium-gray dark:text-white">
                                        Showing {(currentPage - 1) * itemsPerPage + 1} to{" "}
                                        {Math.min(currentPage * itemsPerPage, filteredMetadata.length)} of{" "}
                                        {filteredMetadata.length} files
                                    </div>
                                    <div className="flex gap-2">
                                        <button
                                            onClick={() => setCurrentPage((prev) => Math.max(prev - 1, 1))}
                                            disabled={currentPage === 1}
                                            className={`px-3 py-1 rounded-md ${currentPage === 1
                                                ? "bg-subtle-gray dark:bg-medium-gray/30 text-medium-gray/50 dark:text-white/50 cursor-not-allowed"
                                                : "bg-accent-blue/10 text-accent-blue hover:bg-accent-blue/20"
                                                } transition-colors`}
                                        >
                                            Previous
                                        </button>
                                        <span className="px-3 py-1 bg-subtle-gray dark:bg-medium-gray/30 text-medium-gray dark:text-white rounded-md">
                                            {currentPage}
                                        </span>
                                        <button
                                            onClick={() =>
                                                setCurrentPage((prev) =>
                                                    Math.min(prev + 1, Math.ceil(filteredMetadata.length / itemsPerPage))
                                                )
                                            }
                                            disabled={currentPage >= Math.ceil(filteredMetadata.length / itemsPerPage)}
                                            className={`px-3 py-1 rounded-md ${currentPage >= Math.ceil(filteredMetadata.length / itemsPerPage)
                                                ? "bg-subtle-gray dark:bg-medium-gray/30 text-medium-gray/50 dark:text-white/50 cursor-not-allowed"
                                                : "bg-accent-blue/10 text-accent-blue hover:bg-accent-blue/20"
                                                } transition-colors`}
                                        >
                                            Next
                                        </button>
                                    </div>
                                </div>
                            </div>
                        ) : (
                            <div className="text-center p-8">
                                <p className="text-medium-gray dark:text-white mb-4">
                                    {metadata.length === 0 && objectStorePath
                                        ? "No files found. Try a different path or check your connection."
                                        : "Enter a path and press Browse to view files."}
                                </p>
                            </div>
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