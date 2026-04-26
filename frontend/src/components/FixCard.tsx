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
    <div className="card relative border-[var(--color-border)] hover:border-[var(--color-brand)]">
      <div className="absolute top-0 left-0 w-1 h-full bg-[var(--color-brand)]" />
      
      <div className="flex items-start gap-5">
        <div className="flex-shrink-0 w-8 h-8 border-2 border-[var(--color-brand)] text-[var(--color-brand)] flex items-center justify-center font-bold font-mono">
          {index}
        </div>
        
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-3 mb-2">
            <span className="label-mono font-bold text-[var(--color-brand)] bg-[var(--color-brand)]/10 px-2 py-1 border border-[var(--color-brand)]/20">
              {fix.action.replace(/_/g, ' ')}
            </span>
            <span className="text-[var(--color-text-muted)] text-xs">&rarr;</span>
            <span className="label-mono font-bold truncate border-b border-[var(--color-border)] text-[var(--color-text)]">{fix.target}</span>
          </div>
          
          <p className="text-base font-medium mb-4 mt-3 text-[var(--color-text)]">{fix.description}</p>
          
          {fix.markdown_details && (
            <div className="mb-4 text-sm font-medium bg-[var(--color-bg)] p-4 border border-[var(--color-border)] text-[var(--color-text)]">
              {fix.markdown_details}
            </div>
          )}
          
          {fix.sql_script && (
            <div className="mt-4 border border-[var(--color-border)] bg-[#1e1e1e]">
              <div className="flex items-center justify-between px-4 py-2 border-b border-[#333333] bg-[#252526]">
                <span className="label-mono text-[#cccccc]">remediation.sql</span>
                <button
                  onClick={handleCopy}
                  className="label-mono text-white hover:text-[var(--color-brand)] font-bold flex items-center gap-2 transition-colors"
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
                <pre className="p-4 text-xs font-mono text-[#cccccc] overflow-x-auto whitespace-pre-wrap selection:bg-[var(--color-brand)] selection:text-white">
                  {fix.sql_script}
                </pre>
                {!expandedCode && (
                  <div className="absolute bottom-0 left-0 right-0 h-16 bg-gradient-to-t from-[#1e1e1e] to-transparent flex items-end justify-center pb-3">
                    <button
                      onClick={() => setExpandedCode(true)}
                      className="label-mono text-white bg-[#1e1e1e] px-4 py-1 border border-[#444444] hover:border-white transition-colors"
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
