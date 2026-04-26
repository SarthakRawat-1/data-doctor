import { useState } from "react";
import type { SuggestedFix } from "../types";

interface Props {
  fix: SuggestedFix;
  index: number;
}

export function FixCard({ fix, index }: Props) {
  const [expandedCode, setExpandedCode] = useState(false);
  const [copied, setCopied] = useState(false);

  const handleCopy = async () => {
    if (fix.sql_script) {
      await navigator.clipboard.writeText(fix.sql_script);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    }
  };

  return (
    <div className="card relative border-[var(--color-border)] hover:border-white/30 transition-colors">
      <div className="absolute top-0 left-0 w-1 h-full bg-white/40" />
      
      <div className="flex items-start gap-5">
        <div className="flex-shrink-0 w-8 h-8 border border-white/30 text-white flex items-center justify-center font-bold font-mono text-sm">
          {index}
        </div>
        
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-3 mb-2">
            <span className="label-mono font-bold text-white bg-[rgba(255,255,255,0.05)] px-2 py-1 border border-[var(--color-border)]">
              {fix.action.replace(/_/g, ' ')}
            </span>
            <span className="text-[var(--color-text-muted)] text-xs">&rarr;</span>
            <span className="label-mono font-bold truncate text-[var(--color-text)]">{fix.target}</span>
          </div>
          
          <p className="text-sm font-medium mb-4 mt-3 text-[var(--color-text-muted)] leading-relaxed">{fix.description}</p>
          
          {fix.markdown_details && (
            <div className="mb-4 text-sm font-medium bg-[var(--color-bg)] p-4 border border-[var(--color-border)] text-[var(--color-text-muted)]">
              {fix.markdown_details}
            </div>
          )}
          
          {fix.sql_script && (
            <div className="mt-4 border border-[var(--color-border)] bg-[var(--color-bg)]">
              <div className="flex items-center justify-between px-4 py-2 border-b border-[var(--color-border)] bg-[rgba(255,255,255,0.02)]">
                <span className="label-mono text-[var(--color-text-muted)]">remediation.sql</span>
                <button
                  onClick={handleCopy}
                  className="label-mono text-[var(--color-text-muted)] hover:text-white font-bold flex items-center gap-2 transition-colors"
                >
                  {copied ? (
                    <>
                      <svg className="w-3 h-3" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={3} d="M5 13l4 4L19 7" />
                      </svg>
                      COPIED
                    </>
                  ) : (
                    <>
                      <svg className="w-3 h-3" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z" />
                      </svg>
                      COPY SQL
                    </>
                  )}
                </button>
              </div>
              <div className={`relative ${expandedCode ? '' : 'max-h-40 overflow-hidden'}`}>
                <pre className="p-4 text-xs font-mono text-[var(--color-text-muted)] overflow-x-auto whitespace-pre-wrap selection:bg-white selection:text-black">
                  {fix.sql_script}
                </pre>
                {!expandedCode && (
                  <div className="absolute bottom-0 left-0 right-0 h-16 bg-gradient-to-t from-[var(--color-bg)] to-transparent flex items-end justify-center pb-3">
                    <button
                      onClick={() => setExpandedCode(true)}
                      className="label-mono text-white bg-[var(--color-bg)] px-4 py-1 border border-[var(--color-border)] hover:border-white transition-colors"
                    >
                      SHOW FULL SCRIPT
                    </button>
                  </div>
                )}
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
