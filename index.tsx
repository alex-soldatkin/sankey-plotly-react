
import React, { useState, useEffect, useRef } from 'react';
import { createRoot } from 'react-dom/client';
import Plotly from 'plotly.js-dist-min';

// --- Constants from Python script ---
const STEP_YEARS = 4;
const UNIT_LABEL = 'bln';

// FIX: Define the missing constant EXIT_LABEL to resolve reference errors.
const EXIT_LABEL = 'Exit';

// This order defines the Y-axis sorting, from top to bottom
const CAT_ORDER = ['State ≥50%', 'State 20–50%', 'State 10–20%', 'State 0–10%', 'State 0%', EXIT_LABEL];

const GREEN = 'rgba(76,175,80,0.65)';
const RED = 'rgba(244,67,54,0.70)';
const NODE_COLORS = {
    'State 0%': 'rgba(33,150,243,0.90)',
    'State 0–10%': 'rgba(63,81,181,0.90)',
    'State 10–20%': 'rgba(0,150,136,0.90)',
    'State 20–50%': 'rgba(255,193,7,0.90)',
    'State ≥50%': 'rgba(244,67,54,0.90)',
    [EXIT_LABEL]: 'rgba(120,120,120,0.75)',
};

const FIG_W = 1400;
const FIG_H = 780;
const NODE_PAD = 22;
const NODE_THICK = 18;

// --- New Deterministic Layout Generation ---
const generateDeterministicLayout = () => {
    const YEARS = [2004, 2008, 2012, 2016, 2020, 2024, 2025];
    
    // 1. Define Node structure and Links
    const nodes = [];
    const node_id_map = {};

    for (const year of YEARS) {
        for (const cat of CAT_ORDER) {
            const nid = nodes.length;
            node_id_map[`${year}-${cat}`] = nid;
            nodes.push({
                id: nid,
                label: `${year}: ${cat}`,
                year: year,
                category: cat,
                color: NODE_COLORS[cat],
                value: 0, // Will be calculated based on flows
                x: 0, y: 0 // Will be calculated
            });
        }
    }

    // Generate links with structured randomness to ensure a connected graph
    const raw_links = [];
    for (let i = 0; i < YEARS.length - 1; i++) {
        const y0 = YEARS[i];
        const y1 = YEARS[i+1];
        const active_cats = CAT_ORDER.filter(c => c !== EXIT_LABEL);

        for (let j = 0; j < active_cats.length; j++) {
            const cat_from = active_cats[j];
             // Guaranteed base flow to the same bucket
            raw_links.push({
                source: node_id_map[`${y0}-${cat_from}`],
                target: node_id_map[`${y1}-${cat_from}`],
                weight: Math.random() * 400 + 150,
                flow_type: 'active',
                level_from: y0, level_to: y1,
            });
            // Random smaller flow to an adjacent bucket
            if (j > 0 && Math.random() > 0.6) {
                 raw_links.push({
                    source: node_id_map[`${y0}-${cat_from}`],
                    target: node_id_map[`${y1}-${active_cats[j-1]}`],
                    weight: Math.random() * 150,
                    flow_type: 'active',
                    level_from: y0, level_to: y1,
                });
            }
             // Random exit flow
            if (Math.random() > 0.8) {
                raw_links.push({
                    source: node_id_map[`${y0}-${cat_from}`],
                    target: node_id_map[`${y0}-${EXIT_LABEL}`],
                    weight: Math.random() * 80,
                    flow_type: 'exit',
                    level_from: y0, level_to: y0,
                });
            }
        }
    }

    // 2. Calculate node values based on max flow (in or out)
    for (const link of raw_links) {
        nodes[link.source].value += link.weight;
        nodes[link.target].value += link.weight;
    }
    
     // 3. Calculate X positions (strict columns)
    const LEFT_MARGIN = 0.08, RIGHT_MARGIN = 0.04;
    const dx = (1 - LEFT_MARGIN - RIGHT_MARGIN) / (YEARS.length - 1);
    const xpos = Object.fromEntries(YEARS.map((y, i) => [y, LEFT_MARGIN + i * dx]));
    const EPS_EXIT = 1e-6;

    for (const node of nodes) {
        node.x = xpos[node.year] + (node.category === EXIT_LABEL ? EPS_EXIT : 0.0);
    }
    
    // 4. Calculate Y positions (spaced-out stacked bar layout)
    const Y_TOP_MARGIN = 0.05, Y_BOTTOM_MARGIN = 0.05;
    const VERTICAL_GAP = 0.03;

    for (const year of YEARS) {
        const year_nodes = nodes.filter(n => n.year === year && n.value > 0);
        if (year_nodes.length === 0) continue;

        const total_value_in_year = year_nodes.reduce((sum, n) => sum + n.value, 0);
        const total_gap_space = (year_nodes.length - 1) * VERTICAL_GAP;
        const drawable_height = 1.0 - Y_TOP_MARGIN - Y_BOTTOM_MARGIN - total_gap_space;

        let current_y = Y_TOP_MARGIN;
        for (const cat of CAT_ORDER) {
            const node = year_nodes.find(n => n.category === cat);
            if (node) {
                const node_height = (node.value / total_value_in_year) * drawable_height;
                node.y = current_y + node_height / 2; // Position is the center of the node
                current_y += node_height + VERTICAL_GAP;
            }
        }
    }

    // 5. Finalize links for Plotly
    const step_totals = {};
     for (const link of raw_links) {
        const key = `${link.level_from}-${link.level_to}`;
        step_totals[key] = (step_totals[key] || 0) + link.weight;
    }

    const links = raw_links.map(link => {
        const key = `${link.level_from}-${link.level_to}`;
        const share = step_totals[key] > 0 ? (100 * link.weight) / step_totals[key] : 0;
        return {
            source: link.source,
            target: link.target,
            value_abs: link.weight,
            value_share: share,
            color: link.flow_type === 'active' ? GREEN : RED,
            customdata: [share, link.weight * 1.5], // Mock raw sum
        };
    });

    const active_nodes = nodes.filter(n => n.value > 0);
    
    // Create a mapping from old ID to new index
    const id_to_new_index = Object.fromEntries(active_nodes.map((n, i) => [n.id, i]));
    
    const final_links = links
        .filter(l => id_to_new_index[l.source] !== undefined && id_to_new_index[l.target] !== undefined)
        .map(l => ({
            ...l,
            source: id_to_new_index[l.source],
            target: id_to_new_index[l.target],
        }));

    return {
        nodes: active_nodes.map(n => n.label),
        node_colors: active_nodes.map(n => n.color),
        xs: active_nodes.map(n => n.x),
        ys: active_nodes.map(n => n.y),
        links: final_links,
    };
};


const data = generateDeterministicLayout();
const NODE_X_DEFAULT = data.xs.slice();
const NODE_Y_DEFAULT = data.ys.slice();

const App = () => {
    const plotRef = useRef(null);
    const [thicknessMode, setThicknessMode] = useState('abs'); // 'abs' or 'share'
    const [layoutMode, setLayoutMode] = useState<'fixed' | 'freeform'>('fixed');
    
    const [nodeX, setNodeX] = useState(NODE_X_DEFAULT);
    const [nodeY, setNodeY] = useState(NODE_Y_DEFAULT);

    useEffect(() => {
        if (plotRef.current) {
            const values = data.links.map(l => thicknessMode === 'abs' ? l.value_abs : l.value_share);

            Plotly.newPlot(plotRef.current, [{
                type: 'sankey',
                arrangement: layoutMode,
                domain: { x: [0, 1], y: [0, 1] },
                node: {
                    pad: NODE_PAD,
                    thickness: NODE_THICK,
                    label: data.nodes,
                    color: data.node_colors,
                    x: nodeX,
                    y: nodeY,
                    line: { width: 0.5, color: 'rgba(0,0,0,0.3)' },
                },
                link: {
                    source: data.links.map(l => l.source),
                    target: data.links.map(l => l.target),
                    value: values,
                    color: data.links.map(l => l.color),
                    customdata: data.links.map(l => l.customdata) as any,
                    hovertemplate: 'From %{source.label}<br>' +
                        'To %{target.label}<br>' +
                        `Raw MA sum: %{customdata[1]:,.1f} ${UNIT_LABEL}<br>` +
                        'Thickness value (current mode): %{value:,.2f}<br>' +
                        'Share of step total: %{customdata[0]:.1f}%' +
                        '<extra></extra>',
                },
            }], {
                title: {
                    text: `Ownership buckets (step=${STEP_YEARS}y) — Weight = 12M MA of Total Deposits`,
                    x: 0.02,
                    xanchor: 'left',
                },
                width: FIG_W,
                height: FIG_H,
                margin: { l: 30, r: 30, t: 80, b: 30 },
            }, { responsive: true });

            const plotDiv = plotRef.current as any;
            const listener = (eventData: any) => {
                if (layoutMode === 'freeform' && eventData && eventData['node.x'] && eventData['node.y']) {
                    const newX = eventData['node.x'];
                    const newY = eventData['node.y'];
                    if (JSON.stringify(newX) !== JSON.stringify(nodeX)) {
                        setNodeX(newX);
                    }
                    if (JSON.stringify(newY) !== JSON.stringify(nodeY)) {
                        setNodeY(newY);
                    }
                }
            };

            plotDiv.on('plotly_relayout', listener);

            return () => {
                if (plotDiv) {
                    plotDiv.removeAllListeners('plotly_relayout');
                }
            };
        }
    }, [thicknessMode, layoutMode, nodeX, nodeY]);
    
    const resetPositions = () => {
        setNodeX(NODE_X_DEFAULT);
        setNodeY(NODE_Y_DEFAULT);
    };

    const handleLayoutModeChange = (mode: 'fixed' | 'freeform') => {
        setLayoutMode(mode);
        if (mode === 'fixed') {
            resetPositions();
        }
    };
    
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
                       <button className={`btn ${layoutMode === 'fixed' ? 'active' : ''}`} onClick={() => handleLayoutModeChange('fixed')}>Lock Columns</button>
                       <button className={`btn ${layoutMode === 'freeform' ? 'active' : ''}`} onClick={() => handleLayoutModeChange('freeform')}>Tidy Rows</button>
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

const container = document.getElementById('root');
const root = createRoot(container!);
root.render(<App />);
