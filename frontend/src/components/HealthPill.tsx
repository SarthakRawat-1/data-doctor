import { useEffect, useState } from "react";
import { healthCheck } from "../api";

export function HealthPill() {
  const [status, setStatus] = useState<"loading" | "healthy" | "degraded">("loading");

  useEffect(() => {
    async function check() {
      try {
        const res = await healthCheck();
        setStatus(res.status as "healthy" | "degraded");
      } catch {
        setStatus("degraded");
      }
    }
    check();
    const interval = setInterval(check, 30000);
    return () => clearInterval(interval);
  }, []);

  return (
    <div className="flex items-center gap-2 px-3 py-1 bg-[var(--color-bg-alt)] border border-[var(--color-border)] rounded-sm">
      <span
        className={`w-2 h-2 rounded-none ${
          status === "healthy" ? "bg-[var(--color-low)]" :
          status === "degraded" ? "bg-[var(--color-high)]" :
          "bg-[var(--color-border)]"
        }`}
      />
      <span className="label-mono text-[var(--color-text-muted)]">
        {status === "loading" ? "CHECKING" :
         status === "healthy" ? "CONNECTED" : "DEGRADED"}
      </span>
    </div>
  );
}
