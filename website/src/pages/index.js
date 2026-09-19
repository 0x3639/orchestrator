import React from 'react';
import Link from '@docusaurus/Link';
import Layout from '@theme/Layout';

const features = [
  {
    eyebrow: 'Setup',
    title: 'Install and configure',
    body: 'Build from source or use a release binary, write a config.json, fund the signer address and back up the key share.',
    to: '/docs/install',
    label: 'Start here',
  },
  {
    eyebrow: 'Sync',
    title: 'Survive the first sync',
    body: 'Cap requests per EVM endpoint, size log queries to your provider, and read the progress lines so a multi-hour catch-up is visible.',
    to: '/docs/operations/first-sync',
    label: 'First sync runbook',
  },
  {
    eyebrow: 'Signing',
    title: 'Keep signers converged',
    body: 'Why unwrap signing stalls, how nodes self-heal, and the bounded backfill repair for a signer that has lost events.',
    to: '/docs/operations/signing-stalls',
    label: 'Signing stalls runbook',
  },
];

export default function Home() {
  return (
    <Layout title="Operator guide" description="Operator documentation for the Zenon TSS bridge orchestrator">
      <header className="hero--orchestrator">
        <div className="container text--center">
          <div className="text-ledger hero__eyebrow">Zenon · Network of Momentum · Bridge</div>
          <h1 className="hero__title">Orchestrator operator guide</h1>
          <p className="hero__subtitle">
            Run a TSS bridge signer that stays in sync with Ethereum and Zenon, exposes a private
            health endpoint, and recovers without a hard reset.
          </p>
          <div className="hero__actions">
            <Link className="button button--primary button--lg" to="/docs/intro">
              Read the docs
            </Link>
            <Link className="button button--secondary button--lg" to="/docs/upgrade-notes">
              Upgrade notes
            </Link>
          </div>
        </div>
      </header>
      <main className="container">
        <section className="feature-grid">
          {features.map((f) => (
            <div className="card" key={f.title}>
              <div className="text-ledger">{f.eyebrow}</div>
              <h3>{f.title}</h3>
              <p>{f.body}</p>
              <p style={{marginTop: '0.75rem'}}>
                <Link to={f.to}>{f.label}</Link>
              </p>
            </div>
          ))}
        </section>
      </main>
    </Layout>
  );
}
