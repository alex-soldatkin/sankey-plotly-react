import * as df from 'danfojs/dist/danfojs-browser/src';
import { tableFromIPC } from 'apache-arrow';

// --- Types ---
export interface SankeyLink {
    source: number;
    target: number;
    value_abs: number;
    value_share: number;
    color: string;
    customdata: (string | number)[];
}

export interface SankeyData {
    nodes: string[];
    node_colors: string[];
    xs: number[];
    ys: number[];
    links: SankeyLink[];
    title: string;
}

// --- Parameters ---
const INDICATOR_COL = 'total_deposits';
const MA_MONTHS = 12;
const STEP_YEARS = 4;

const OWN_BUCKETS = [
    { lo: 50.0, hi: 100.1, name: 'State ≥50%' },
    { lo: 20.0, hi: 50.0, name: 'State 20–50%' },
    { lo: 10.0, hi: 20.0, name: 'State 10–20%' },
    { lo: 0.0, hi: 10.0, name: 'State 0–10%' },
    { lo: 0.0, hi: 0.0, name: 'State 0%' },
];
const UNKNOWN_LABEL = 'Unknown';
const EXIT_LABEL = 'Exit';

const UNIT_SCALE = 1e9;

const TRANSFORM_MODE = 'signed_log';
const CLIP_ENABLED = true;
const CLIP_LO = 0.02;
const CLIP_HI = 0.98;

const GREEN = 'rgba(76,175,80,0.65)';
const RED = 'rgba(244,67,54,0.70)';
const NODE_COLORS: { [key: string]: string } = {
    'State ≥50%': 'rgba(244,67,54,0.90)',
    'State 20–50%': 'rgba(255,193,7,0.90)',
    'State 10–20%': 'rgba(0,150,136,0.90)',
    'State 0–10%': 'rgba(63,81,181,0.90)',
    'State 0%': 'rgba(33,150,243,0.90)',
    [UNKNOWN_LABEL]: 'rgba(158,158,158,0.80)',
    [EXIT_LABEL]: 'rgba(120,120,120,0.75)',
};
const CAT_ORDER = [...OWN_BUCKETS.map(b => b.name), UNKNOWN_LABEL, EXIT_LABEL];


// --- Utils ---

function standardizePercent(series: df.Series): df.Series {
    let s = series.asType('float32');
    const nonNa = s.count();
    if (nonNa === 0) return s;

    const arr = s.dropna().values as number[];
    if (arr.length === 0) return s;
    
    // Sort by absolute value to find percentile
    const absArr = arr.map(v => Math.abs(v)).sort((a, b) => a - b);
    const p95_val = absArr[Math.floor(0.95 * (absArr.length - 1))];
    
    if (p95_val > 1.5) return s;
    return s.map((val: number) => val * 100.0);
}

function yearEndSnapshotFfill(dataframe: df.DataFrame, cols: string[]): df.DataFrame {
    let tmp = dataframe.copy();
    tmp.addColumn('year', tmp['DT'].dt.year(), { inplace: true });
    tmp.sortValues(['REGN', 'year', 'DT'], { inplace: true });

    const uniqueKeysDf = tmp.loc({ columns: ['REGN', 'year'] }).unique();
    if (uniqueKeysDf.shape[0] === 0) {
        const emptyCols = ['REGN', 'DT', 'year', ...cols];
        return new df.DataFrame([], { columns: emptyCols });
    }

    const filledDfs: df.DataFrame[] = [];

    for (let i = 0; i < uniqueKeysDf.shape[0]; i++) {
        const row = uniqueKeysDf.iloc({ rows: [i] });
        const regn = row['REGN'].values[0];
        const year = row['year'].values[0];
        
        let group = tmp.query(tmp['REGN'].eq(regn).and(tmp['year'].eq(year)));
        
        for (const c of cols) {
            const filledSeries = group[c].fillNa({ method: 'ffill', inplace: false });
            group.drop({ columns: [c], inplace: true });
            group.addColumn(c, filledSeries, { inplace: true });
        }
        filledDfs.push(group);
    }
    
    if (filledDfs.length === 0) {
        const emptyCols = ['REGN', 'DT', 'year', ...cols];
        return new df.DataFrame([], { columns: emptyCols });
    }

    const filledDf = df.concat({ dfList: filledDfs, axis: 0 });
    // CRITICAL FIX: Re-sort after concat to ensure correct order before taking tail.
    filledDf.sortValues(['REGN', 'year', 'DT'], { inplace: true });

    const finalGrouped = filledDf.groupby(['REGN', 'year']);
    const snapshot = finalGrouped.tail(1);
    return snapshot.loc({ columns: ['REGN', 'DT', 'year', ...cols] });
}


function buildYearGrid(min_year: number, max_year: number, step: number): number[] {
    const years: number[] = [];
    for (let y = min_year; y <= max_year; y += step) {
        years.push(y);
    }
    if (years.length === 0 || years[years.length - 1] < max_year) {
        years.push(max_year);
    }
    return years;
}

function getBucket(p: number | null | undefined): string {
    if (p === null || p === undefined || !isFinite(p)) return UNKNOWN_LABEL;
    if (Math.abs(p) < 1e-12) return 'State 0%';

    const bucket = OWN_BUCKETS.find(b => b.name !== 'State 0%' && p > b.lo - 1e-12 && p < b.hi + 1e-12);
    return bucket ? bucket.name : UNKNOWN_LABEL;
}

// --- Main Processing Function ---
export async function processParquetData(fileBuffer: ArrayBuffer): Promise<SankeyData> {
    
    // 1. Load data
    const table = tableFromIPC(fileBuffer);
    let DF = new df.DataFrame(table.toArray().map(row => row.toJSON()));
    
    for (const c of ['DT', 'REGN', INDICATOR_COL, 'state_equity_pct']) {
        if (!DF.columns.includes(c)) throw new Error(`Expected column '${c}' not found in data`);
    }

    DF['DT'] = df.toDateTime(DF['DT']);
    DF['REGN'] = df.toNumeric(DF['REGN']);
    DF[INDICATOR_COL] = df.toNumeric(DF[INDICATOR_COL]);
    DF['state_equity_pct'] = standardizePercent(DF['state_equity_pct']);
    
    // 2. Preprocess Data
    DF.sortValues(['REGN', 'DT'], { inplace: true });
    const min_periods = Math.max(1, Math.floor(MA_MONTHS / 3));

    // CRITICAL FIX: Apply rolling mean group by group in a sorted order to ensure correct alignment.
    const groupedByRegn = DF.groupby(['REGN']);
    const uniqueRegns = DF['REGN'].unique();
    uniqueRegns.sortValues({ inplace: true }); // Sort keys to match DataFrame order
    const regnKeys = uniqueRegns.values;
    
    const maValues: (number | null)[] = [];
    for (const key of regnKeys) {
        const group = groupedByRegn.getGroup([key]);
        const ma = group[INDICATOR_COL].rolling(MA_MONTHS, { minPeriods: min_periods }).mean();
        maValues.push(...(ma.values as (number|null)[]));
    }
    DF.addColumn('indicator_ma', maValues, { inplace: true });

    const SNAP = yearEndSnapshotFfill(DF, ['state_equity_pct', 'indicator_ma']);
    const RAW_SNAP = yearEndSnapshotFfill(DF, [INDICATOR_COL]);
    
    const mergedSnap = df.merge({
        left: SNAP,
        right: RAW_SNAP.loc({ columns: ['REGN', 'year', INDICATOR_COL] })
            .rename({ [INDICATOR_COL]: 'indicator_raw' }),
        on: ['REGN', 'year'],
        how: 'left'
    });

    const indicatorMaFilled = mergedSnap['indicator_ma'].isNa().values.map((isNa, i) => 
        isNa ? mergedSnap['indicator_raw'].values[i] : mergedSnap['indicator_ma'].values[i]
    );
    mergedSnap.drop({ columns: ['indicator_ma'], inplace: true });
    mergedSnap.addColumn('indicator_ma', indicatorMaFilled, { inplace: true });
    
    let s = df.toNumeric(mergedSnap['indicator_ma']);
    let t;
    if (TRANSFORM_MODE === 'signed_log') {
        t = s.abs().add(1).log();
    } else {
        t = s;
    }
    t = t.clip({ lower: 0, inplace: false });

    mergedSnap.addColumn('w_t', t, { inplace: true });
    mergedSnap.addColumn('w_raw', s.clip({ lower: 0, inplace: false }), { inplace: true });

    if (CLIP_ENABLED) {
        const arr = mergedSnap['w_t'].dropna().values as number[];
        if (arr.length > 0) {
            arr.sort((a,b) => a - b);
            const lo = arr[Math.floor(CLIP_LO * (arr.length - 1))];
            const hi = arr[Math.floor(CLIP_HI * (arr.length - 1))];
            if (isFinite(lo) && isFinite(hi) && hi > lo) {
                mergedSnap['w_t'].clip({ lower: lo, upper: hi, inplace: true });
            }
        }
    }

    mergedSnap.addColumn('bucket', mergedSnap['state_equity_pct'].map(getBucket), { inplace: true });
    
    // 3. Build Flows
    const min_year = mergedSnap['year'].min();
    const max_year = mergedSnap['year'].max();
    const YEARS = buildYearGrid(min_year, max_year, STEP_YEARS);

    if (YEARS.length < 2) throw new Error('Not enough annual steps to build a time-sankey.');

    const linkRows: any[] = [];
    for (let i = 0; i < YEARS.length - 1; i++) {
        const y0 = YEARS[i];
        const y1 = YEARS[i+1];

        const s0 = mergedSnap.query(mergedSnap['year'].eq(y0))
            .rename({ 'bucket': 'cat_from', 'w_t': 'w', 'w_raw': 'raw' })
            .loc({ columns: ['REGN', 'cat_from', 'w', 'raw'] });

        const s1 = mergedSnap.query(mergedSnap['year'].eq(y1))
            .rename({ 'bucket': 'cat_to' })
            .loc({ columns: ['REGN', 'cat_to'] });
        
        // Active flows
        const both = df.merge({ left: s0, right: s1, on: ['REGN'], how: 'inner' }).dropna();
        if (both.shape[0] > 0) {
            const agg = both.groupby(['cat_from', 'cat_to'])
                .agg({ w: ['sum'], REGN: ['count'], raw: ['sum'] });
            agg.rename({
                "w_sum": "weight",
                "REGN_count": "count",
                "raw_sum": "raw_sum"
            }, { inplace: true });
            agg['level_from'] = y0;
            agg['level_to'] = y1;
            agg['flow_type'] = 'active';
            linkRows.push(...df.toJSON(agg) as any[]);
        }

        // Exit flows
        const gone = df.merge({ left: s0, right: s1.loc({columns: ['REGN']}), on: ['REGN'], how: 'left', indicator: true });
        const exits = gone.query(gone['_merge'].eq('left_only'));
        if (exits.shape[0] > 0) {
             const agx = exits.groupby(['cat_from'])
                .agg({ w: ['sum'], REGN: ['count'], raw: ['sum'] });
            agx.rename({
                "w_sum": "weight",
                "REGN_count": "count",
                "raw_sum": "raw_sum"
            }, { inplace: true });
            agx['cat_to'] = EXIT_LABEL;
            agx['level_from'] = y0;
            agx['level_to'] = y0; // Exit is in the same year
            agx['flow_type'] = 'exit';
            linkRows.push(...df.toJSON(agx) as any[]);
        }
    }

    if (linkRows.length === 0) throw new Error('No flows computed. Check data availability across 4-year steps.');
    const linksDf = new df.DataFrame(linkRows);

    // 4. Build Nodes
    const nodes: string[] = [];
    const node_id: { [key: string]: number } = {};
    const node_colors: string[] = [];
    
    // Generate all potential nodes first
    YEARS.forEach(year => {
        CAT_ORDER.forEach(cat => {
            const nodeIdKey = `${year}: ${cat}`;
            if (!(nodeIdKey in node_id)) {
                node_id[nodeIdKey] = nodes.length;
                nodes.push(nodeIdKey);
                node_colors.push(NODE_COLORS[cat] || 'rgba(150,150,150,0.85)');
            }
        });
    });

    // 5. Build Links
    const source = linksDf['level_from'].values.map((y, i) => node_id[`${y}: ${linksDf['cat_from'].values[i]}`]);
    const target = linksDf['level_to'].values.map((y, i) => node_id[`${y}: ${linksDf['cat_to'].values[i]}`]);
    linksDf.addColumn('source', source, { inplace: true });
    linksDf.addColumn('target', target, { inplace: true });
    
    const stepTotalsGrouped = linksDf.groupby(['level_from', 'level_to']);
    const stepTotals = stepTotalsGrouped['weight'].transform('sum', { inplace: false });
    linksDf.addColumn('step_total', stepTotals, { inplace: true });

    const value_abs = linksDf['weight'].div(UNIT_SCALE);
    const stepTotalsArr = linksDf['step_total'].values;
    const value_share = linksDf['weight'].map(
        (w, i) => (stepTotalsArr[i] > 1e-9 ? (w / (stepTotalsArr[i] as number)) * 100 : 0)
    );
    const raw_abs = linksDf['raw_sum'].div(UNIT_SCALE);

    const links: SankeyLink[] = (df.toJSON(linksDf) as any[]).map((l: any, i) => ({
        source: l.source,
        target: l.target,
        weight: l.weight,
        value_abs: value_abs.values[i],
        value_share: value_share.values[i] || 0,
        color: l.flow_type === 'active' ? GREEN : RED,
        customdata: [
            value_share.values[i] || 0,
            raw_abs.values[i] || 0,
        ]
    }));
    
    // 6. Calculate deterministic layout
    const LEFT_MARGIN = 0.05, RIGHT_MARGIN = 0.05;
    const dx = (1 - LEFT_MARGIN - RIGHT_MARGIN) / (YEARS.length - 1);
    const xpos = YEARS.reduce((acc, y, i) => {
        acc[y] = LEFT_MARGIN + i * dx;
        return acc;
    }, {} as { [key: number]: number });
    
    const EPS_EXIT = 1e-6;
    const xs = nodes.map(n => {
        const [yearStr, cat] = n.split(': ');
        const year = parseInt(yearStr);
        return xpos[year] + (cat === EXIT_LABEL ? EPS_EXIT : 0);
    });

    // Deterministic Y position based on flow
    const nodeWeights = new Array(nodes.length).fill(0);
    links.forEach((l: any) => {
        if (typeof l.source === 'number' && typeof l.target === 'number') {
            nodeWeights[l.source] += l.weight || 0;
            nodeWeights[l.target] += l.weight || 0;
        }
    });

    const ys = new Array(nodes.length).fill(0.5);
    const TOP_MARGIN = 0.05, BOTTOM_MARGIN = 0.05;
    const TOTAL_Y_SPACE = 1 - TOP_MARGIN - BOTTOM_MARGIN;

    YEARS.forEach(year => {
        const yearNodeIndices = nodes.map((n, i) => n.startsWith(`${year}:`) ? i : -1).filter(i => i !== -1);
        
        const sortedYearNodeIndices = yearNodeIndices.sort((a, b) => {
            const catA = nodes[a].split(': ')[1];
            const catB = nodes[b].split(': ')[1];
            return CAT_ORDER.indexOf(catA) - CAT_ORDER.indexOf(catB);
        });

        const totalColumnWeight = sortedYearNodeIndices.reduce((sum, i) => sum + nodeWeights[i], 0);
        
        if (totalColumnWeight > 1e-9) {
            let currentY = TOP_MARGIN;
            sortedYearNodeIndices.forEach(nodeIdx => {
                const nodeWeight = nodeWeights[nodeIdx];
                const nodeHeight = (nodeWeight / totalColumnWeight) * TOTAL_Y_SPACE;
                ys[nodeIdx] = 1 - (currentY + nodeHeight / 2); // Plotly Y is 0 at bottom, 1 at top
                currentY += nodeHeight;
            });
        } else {
            // If no flow in this year, space nodes evenly
            const numNodes = sortedYearNodeIndices.length;
            if (numNodes > 0) {
                const step = TOTAL_Y_SPACE / numNodes;
                sortedYearNodeIndices.forEach((nodeIdx, i) => {
                    ys[nodeIdx] = 1 - (TOP_MARGIN + i * step + step / 2);
                });
            }
        }
    });

    const title = `Ownership buckets (step=${STEP_YEARS}y) — Weight = ${MA_MONTHS}M MA of ${INDICATOR_COL}`;

    return { nodes, node_colors, xs, ys, links, title };
}
