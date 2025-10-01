import React, { useState, useEffect, useRef, useCallback } from 'react';
import { createRoot } from 'react-dom/client';
import Plotly from 'plotly.js-dist-min';
import { processParquetData, SankeyData } from './dataProcessor';

// --- Constants ---
const FIG_W = 1400;
const FIG_H = 780;
const NODE_PAD = 22;
const NODE_THICK = 18;
const UNIT_LABEL = 'bln';


// --- Chart Component ---
const SankeyChart = ({ chartData }: { chartData: SankeyData }) => {
    const plotRef = useRef<HTMLDivElement>(null);
    const [thicknessMode, setThicknessMode] = useState<'abs' | 'share'>('abs');
    const [layoutMode, setLayoutMode] = useState<'fixed' | 'freeform'>('fixed');
    
    const [nodeX, setNodeX] = useState(chartData.xs);
    const [nodeY, setNodeY] = useState(chartData.ys);

    // Store original calculated positions for reset
    const NODE_X_DEFAULT = useRef(chartData.xs).current;
    const NODE_Y_DEFAULT = useRef(chartData.ys).current;

    // Update internal state if chartData prop changes
    useEffect(() => {
        setNodeX(chartData.xs);
        setNodeY(chartData.ys);
    }, [chartData]);

    useEffect(() => {
        if (plotRef.current && chartData) {
            const values = chartData.links.map(l => thicknessMode === 'abs' ? l.value_abs : l.value_share);
            
            const data: Partial<Plotly.SankeyData> = {
                type: 'sankey',
                arrangement: layoutMode,
                domain: { x: [0, 1], y: [0, 1] },
                node: {
                    pad: NODE_PAD,
                    thickness: NODE_THICK,
                    label: chartData.nodes,
                    color: chartData.node_colors,
                    x: nodeX,
                    y: nodeY,
                    line: { width: 0.5, color: 'rgba(0,0,0,0.3)' },
                },
                link: {
                    source: chartData.links.map(l => l.source),
                    target: chartData.links.map(l => l.target),
                    value: values,
                    color: chartData.links.map(l => l.color),
                    customdata: chartData.links.map(l => l.customdata) as any,
                    hovertemplate: 'From %{source.label}<br>' +
                        'To %{target.label}<br>' +
                        `Raw MA sum: %{customdata[1]:,.1f} ${UNIT_LABEL}<br>` +
                        'Thickness value (current mode): %{value:,.2f}<br>' +
                        'Share of step total: %{customdata[0]:.1f}%' +
                        '<extra></extra>', // This <extra> tag is needed to show the hover label
                },
            };

            const layout: Partial<Plotly.Layout> = {
                title: { text: chartData.title, x: 0.02, xanchor: 'left' },
                width: FIG_W,
                height: FIG_H,
                margin: { l: 30, r: 30, t: 80, b: 30 },
            };

            Plotly.react(plotRef.current, [data], layout, { responsive: true });

            const plotDiv = plotRef.current as any;
            
            // Event listener for snapping back dragged nodes
            const restyleListener = (eventData: any) => {
                if (layoutMode === 'freeform' && eventData && eventData['node.y']) {
                    const newY = eventData['node.y'][0];
                    // Keep the default X positions to prevent horizontal dragging
                    setNodeX(NODE_X_DEFAULT);
                    setNodeY(newY);
                }
            };
            
            plotDiv.on('plotly_restyle', restyleListener);

            return () => {
                if (plotDiv.removeListener) {
                    plotDiv.removeListener('plotly_restyle', restyleListener);
                }
            };
        }
    }, [chartData, thicknessMode, layoutMode, nodeX, nodeY, NODE_X_DEFAULT]);

    const resetPositions = useCallback(() => {
        setLayoutMode('fixed');
        setNodeX(NODE_X_DEFAULT);
        setNodeY(NODE_Y_DEFAULT);
    }, [NODE_X_DEFAULT, NODE_Y_DEFAULT]);

    return (
        <div className="container">
            <div className="controls">
                <div className="control-group">
                    <label>Thickness</label>
                    <div className="btn-group">
                        <button className={`btn ${thicknessMode === 'abs' ? 'active' : ''}`} onClick={() => setThicknessMode('abs')}>Absolute ({UNIT_LABEL})</button>
                        <button className={`btn ${thicknessMode === 'share' ? 'active' : ''}`} onClick={() => setThicknessMode('share')}>Share (%)</button>
                    </div>
                </div>
                <div className="control-group">
                    <label>Layout</label>
                    <div className="btn-group">
                        <button className={`btn ${layoutMode === 'fixed' ? 'active' : ''}`} onClick={() => setLayoutMode('fixed')}>Lock Columns</button>
                        <button className={`btn ${layoutMode === 'freeform' ? 'active' : ''}`} onClick={() => setLayoutMode('freeform')}>Tidy Rows</button>
                    </div>
                </div>
                <div className="control-group">
                    <label>Actions</label>
                    <div className="btn-group">
                        <button className="btn reset-btn" onClick={resetPositions}>Reset Positions</button>
                    </div>
                </div>
            </div>
            <div ref={plotRef} />
        </div>
    );
};

// --- Main App Component ---
const App = () => {
    const [chartData, setChartData] = useState<SankeyData | null>(null);
    const [isLoading, setIsLoading] = useState(false);
    const [error, setError] = useState<string | null>(null);

    const handleFileUpload = async (event: React.ChangeEvent<HTMLInputElement>) => {
        const file = event.target.files?.[0];
        if (!file) return;

        setIsLoading(true);
        setError(null);
        setChartData(null);

        try {
            const buffer = await file.arrayBuffer();
            const data = await processParquetData(buffer);
            setChartData(data);
        } catch (err) {
            console.error(err);
            const message = err instanceof Error ? err.message : 'An unknown error occurred during data processing.';
            setError(`Failed to process file: ${message}. Please ensure it is the correct Parquet file and format.`);
        } finally {
            setIsLoading(false);
        }
    };

    return (
        <>
            {!chartData && !isLoading && !error && (
                <div className="container upload-container">
                    <h2>Sankey Diagram Generator</h2>
                    <p>Please upload the <code>HOORRAAH_final_banking_indicators_preprocessed.parquet</code> file.</p>
                    <input type="file" accept=".parquet" onChange={handleFileUpload} />
                </div>
            )}
            {isLoading && <div className="container loading-container"><p>Processing data...</p></div>}
            {error && <div className="container error-container"><h3>Error</h3><p>{error}</p></div>}
            {chartData && <SankeyChart chartData={chartData} />}
        </>
    );
};

const container = document.getElementById('root');
if (container) {
    const root = createRoot(container);
    root.render(<App />);
}
