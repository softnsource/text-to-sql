import { useState } from 'react';
import Connect from './components/Connect';
import Training from './components/Training';
import Chat from './components/Chat';
import Auth from './components/Auth';
import Dashboard from './components/Dashboard';
import './App.css';

function App() {
  const [token, setToken] = useState(localStorage.getItem('token') || null);
  const [page, setPage] = useState(token ? 'dashboard' : 'auth');
  const [sessionInfo, setSessionInfo] = useState({
    sessionId: null,
    dialect: null,
    dbName: null
  });

  const handleLogin = (newToken) => {
    localStorage.setItem('token', newToken);
    setToken(newToken);
    setPage('dashboard');
  };

  const disconnect = async () => {
    if (sessionInfo.sessionId) {
      try {
        await fetch(`/api/session/${sessionInfo.sessionId}`, {
          method: 'DELETE'
        });
      } catch (err) {
        console.error("Disconnect error:", err);
      }
    }
    setSessionInfo({ sessionId: null, dialect: null, dbName: null });
    setPage('dashboard');
  };

  const handleLogout = () => {
    localStorage.removeItem('token');
    setToken(null);
    setPage('auth');
  };

  const handleSaveAndChat = async () => {
    if (token && sessionInfo.sessionId) {
      try {
        await fetch(`/api/models/save`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${token}`
          },
          body: JSON.stringify({
            session_id: sessionInfo.sessionId,
            name: `${sessionInfo.dbName} Analytics`
          })
        });
      } catch (err) {
        console.error("Failed to auto-save model", err);
      }
    }
    setPage('chat');
  };

  const loadSavedModel = (info) => {
    setSessionInfo(info);
    setPage('chat');
  };

  return (
    <div className="app-container">
      <nav className="glass-panel navbar">
        <div className="logo" style={{cursor: 'pointer'}} onClick={() => token ? setPage('dashboard') : setPage('auth')}>
          🗄️ Universal DB Chatbot
        </div>
        <div style={{display:'flex', gap: '10px'}}>
          {page === 'chat' && (
             <button className="secondary btn-sm" onClick={disconnect}>🔌 Disconnect & Dashboard</button>
          )}
          {token && (
             <button className="secondary btn-sm" onClick={handleLogout}>🚪 Logout</button>
          )}
        </div>
      </nav>
      
      <main className="main-content">
        {!token && (page === 'auth' || !token) && <Auth onLogin={handleLogin} />}
        {token && page === 'dashboard' && <Dashboard token={token} onLoadModel={loadSavedModel} onConnectNew={() => setPage('connect')} />}
        {token && page === 'connect' && <Connect onConnected={(info) => { setSessionInfo(info); setPage('training'); }} />}
        {token && page === 'training' && <Training sessionInfo={sessionInfo} onComplete={handleSaveAndChat} onError={disconnect} />}
        {token && page === 'chat' && <Chat sessionInfo={sessionInfo} />}
      </main>
    </div>
  );
}

export default App;
