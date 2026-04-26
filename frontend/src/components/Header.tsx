import { Link } from "react-router-dom";
import { HealthPill } from "./HealthPill";

export function Header() {
  return (
    <header className="sticky top-0 z-50 border-b border-[var(--color-border)] bg-[var(--color-bg)]">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 h-20 flex items-center justify-between">
        <Link to="/" className="flex items-center gap-4 group">
          <img 
            src="/logo.png" 
            alt="Data Doctor Logo" 
            className="w-12 h-12 object-contain transition-transform group-hover:scale-105"
          />
          <div>
            <h1 className="text-2xl font-header font-bold text-[var(--color-text)] tracking-tight leading-none">Data Doctor</h1>
          </div>
        </Link>
        
        <div className="flex items-center gap-4">
          <HealthPill />
        </div>
      </div>
    </header>
  );
}
