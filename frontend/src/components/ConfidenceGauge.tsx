import { useEffect, useState } from "react";

interface Props {
  score: number; // 0.0 to 1.0
  className?: string;
}

export function ConfidenceGauge({ score, className = "" }: Props) {
  const [animatedScore, setAnimatedScore] = useState(0);

  useEffect(() => {
    // Slight delay for animation effect
    const timer = setTimeout(() => setAnimatedScore(score), 100);
    return () => clearTimeout(timer);
  }, [score]);

  const radius = 40;
  const circumference = 2 * Math.PI * radius;
  const strokeDashoffset = circumference - animatedScore * circumference;

  let color = "var(--color-low)";
  if (score < 0.5) color = "var(--color-high)";
  else if (score < 0.8) color = "var(--color-medium)";

  return (
    <div className={`relative flex flex-col items-center justify-center ${className}`}>
      <svg className="w-24 h-24 transform -rotate-90">
        <circle
          cx="48"
          cy="48"
          r={radius}
          stroke="var(--color-border)"
          strokeWidth="6"
          fill="none"
        />
        <circle
          cx="48"
          cy="48"
          r={radius}
          stroke={color}
          strokeWidth="6"
          fill="none"
          strokeDasharray={circumference}
          strokeDashoffset={strokeDashoffset}
          strokeLinecap="butt"
          className="transition-all duration-700 ease-out"
        />
      </svg>
      <div className="absolute flex flex-col items-center justify-center">
        <span className="font-mono font-bold text-lg text-[var(--color-text)] tracking-tighter">
          {Math.round(animatedScore * 100)}%
        </span>
      </div>
    </div>
  );
}
