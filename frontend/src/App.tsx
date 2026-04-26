import { BrowserRouter, Routes, Route } from "react-router-dom";
import { Header } from "./components/Header";
import { Home } from "./pages/Home";
import { Diagnose } from "./pages/Diagnose";
import { Results } from "./pages/Results";

function App() {
  return (
    <BrowserRouter>
      <div className="min-h-screen flex flex-col bg-[var(--color-bg)] text-[var(--color-text)]">
        <Header />
        <main className="flex-1">
          <Routes>
            <Route path="/" element={<Home />} />
            <Route path="/diagnose" element={<Diagnose />} />
            <Route path="/results" element={<Results />} />
          </Routes>
        </main>
      </div>
    </BrowserRouter>
  );
}

export default App;
