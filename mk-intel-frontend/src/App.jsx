import { BrowserRouter, Routes, Route } from 'react-router-dom'
import { Navbar } from './components/Navbar'
import { Landing } from './pages/Landing'
import { Examples } from './pages/Examples'
import { ExampleDetail } from './pages/ExampleDetail'
import { TARDetail } from './pages/TARDetail'
import { Setup } from './pages/Setup'
import { Processing } from './pages/Processing'
import { SessionDetail } from './pages/SessionDetail'
import { useState } from 'react'

export default function App() {
  const [session, setSession] = useState(null)
  return (
    <BrowserRouter>
      <div className="min-h-screen flex flex-col">
        <Navbar session={session} />
        <main className="flex-1">
          <Routes>
            <Route path="/" element={<Landing />} />
            <Route path="/examples" element={<Examples />} />
            <Route path="/examples/:slug" element={<ExampleDetail />} />
            <Route path="/examples/:slug/tars/:tarId" element={<TARDetail />} />
            <Route path="/setup" element={<Setup />} />
            <Route path="/processing/:sessionId" element={<Processing />} />
            <Route path="/session/:sessionId" element={<SessionDetail />} />
            <Route path="/session/:sessionId/tars/:tarId" element={<TARDetail source="session" />} />
          </Routes>
        </main>
      </div>
    </BrowserRouter>
  )
}
