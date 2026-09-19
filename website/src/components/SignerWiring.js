import React from 'react';

// Inline SVG so the design-system tokens and fonts from the page apply and
// the diagram follows the light/dark theme. Data (paths, ports, URLs) is set
// in the mono face; labels in the sans face, per the brand rules.
export default function SignerWiring() {
  const sans = 'var(--font-sans)';
  const mono = 'var(--font-mono)';
  const box = {fill: 'var(--card)', stroke: 'var(--border)', strokeWidth: 1, rx: 10};
  const label = {fontFamily: sans, fontSize: 15, fontWeight: 600, fill: 'var(--foreground)'};
  const data = {fontFamily: mono, fontSize: 12.5, fill: 'var(--muted-foreground)'};
  const eyebrow = {fontFamily: mono, fontSize: 10.5, fontWeight: 500, letterSpacing: '0.06em', fill: 'var(--muted-foreground)'};
  const wire = {stroke: 'var(--ifm-color-primary)', strokeWidth: 1.5, fill: 'none', markerStart: 'url(#arrow-start)', markerEnd: 'url(#arrow-end)'};
  const drop = {stroke: 'var(--border)', strokeWidth: 1.5, fill: 'none'};

  return (
    <figure style={{margin: '1.5rem 0'}}>
      <svg
        viewBox="0 0 800 330"
        role="img"
        aria-labelledby="signer-wiring-title"
        style={{width: '100%', height: 'auto', display: 'block'}}
      >
        <title id="signer-wiring-title">
          The orchestrator connects to a Zenon node and to EVM endpoints, keeps its state in the data directory, and exposes a peer port and a loopback health port.
        </title>
        <defs>
          <marker id="arrow-end" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto">
            <path d="M1 1 L9 5 L1 9" fill="none" stroke="var(--ifm-color-primary)" strokeWidth="1.5" />
          </marker>
          <marker id="arrow-start" viewBox="0 0 10 10" refX="1" refY="5" markerWidth="7" markerHeight="7" orient="auto">
            <path d="M9 1 L1 5 L9 9" fill="none" stroke="var(--ifm-color-primary)" strokeWidth="1.5" />
          </marker>
        </defs>

        {/* Zenon node */}
        <rect x="8" y="60" width="180" height="84" {...box} />
        <text x="24" y="84" style={eyebrow}>UPSTREAM</text>
        <text x="24" y="106" style={label}>Zenon node</text>
        <text x="24" y="128" style={data}>ws://127.0.0.1:35998</text>

        {/* EVM endpoints */}
        <rect x="612" y="60" width="180" height="84" {...box} />
        <text x="628" y="84" style={eyebrow}>PER EVM NETWORK</text>
        <text x="628" y="106" style={label}>EVM endpoints</text>
        <text x="628" y="128" style={data}>2 or more providers</text>

        {/* Orchestrator */}
        <rect x="228" y="24" width="344" height="196" {...box} />
        <text x="244" y="48" style={eyebrow}>THIS NODE</text>
        <text x="244" y="70" style={label}>orchestrator</text>
        <text x="244" y="96" style={{...data, fill: 'var(--foreground)'}}>~/.orchestrator/</text>
        <text x="260" y="118" style={data}>config.json</text>
        <text x="260" y="138" style={data}>producer</text>
        <text x="260" y="158" style={data}>events/</text>
        <text x="260" y="178" style={data}>queues/</text>
        <text x="260" y="198" style={data}>tss/</text>
        <text x="356" y="118" style={data}>settings, mode 0600</text>
        <text x="356" y="138" style={data}>encrypted producer key</text>
        <text x="356" y="158" style={data}>event records, sync cursor</text>
        <text x="356" y="178" style={data}>events awaiting finality</text>
        <text x="356" y="198" style={data}>TSS key share</text>

        {/* wires */}
        <line x1="188" y1="102" x2="228" y2="102" style={wire} />
        <line x1="572" y1="102" x2="612" y2="102" style={wire} />

        {/* ports */}
        <line x1="316" y1="220" x2="316" y2="252" style={drop} />
        <line x1="484" y1="220" x2="484" y2="252" style={drop} />
        <rect x="228" y="252" width="176" height="56" {...box} />
        <text x="244" y="275" style={{...data, fill: 'var(--foreground)'}}>:55055</text>
        <text x="244" y="295" style={data}>libp2p, TSS peers</text>
        <rect x="396" y="252" width="176" height="56" {...box} />
        <text x="412" y="275" style={{...data, fill: 'var(--foreground)'}}>:55000</text>
        <text x="412" y="295" style={data}>health RPC, loopback</text>
      </svg>
    </figure>
  );
}
