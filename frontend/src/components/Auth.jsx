import { useState } from 'react';


function Auth({ onLogin }) {
  const [isLogin, setIsLogin] = useState(true);
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState(null);

  const handleSubmit = async (e) => {
    e.preventDefault();
    setError(null);
    try {
      if (!isLogin) {
        const resp = await fetch(`/api/auth/register`, {
           method: 'POST',
           headers: { 'Content-Type': 'application/json' },
           body: JSON.stringify({ username, password })
        });
        if (!resp.ok) {
           const err = await resp.json();
           throw new Error(err.detail);
        }
        setIsLogin(true);
        setError("Registration active. Please hit Login to continue.");
        return;
      } else {
        const formData = new URLSearchParams();
        formData.append('username', username);
        formData.append('password', password);

        const resp = await fetch(`/api/auth/login`, {
           method: 'POST',
           headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
           body: formData
        });
        if (!resp.ok) {
           const err = await resp.json();
           throw new Error(err.detail);
        }
        const data = await resp.json();
        onLogin(data.access_token);
      }
    } catch (err) {
      setError(err.message);
    }
  };

  return (
    <div className="connect-panel glass-panel animate-fade-in" style={{ marginTop: '10vh' }}>
      <div className="header-text">
        <h1>{isLogin ? "Welcome Back" : "Create Account"}</h1>
        <p className="text-muted">Sign in to manage and query your database models</p>
      </div>

      <form onSubmit={handleSubmit}>
        <div className="form-group">
          <label>Username</label>
          <input 
            type="text" 
            required
            value={username}
            onChange={(e) => setUsername(e.target.value)}
          />
        </div>
        <div className="form-group">
          <label>Password</label>
          <input 
            type="password"
            required
            value={password}
            onChange={(e) => setPassword(e.target.value)}
          />
        </div>

        {error && <div className="error-box" style={{marginBottom: '1rem', marginTop:0}}>{error}</div>}

        <button type="submit" style={{width: '100%', marginBottom: '1rem'}}>
          {isLogin ? "Login" : "Register"}
        </button>

        <div style={{textAlign: 'center', fontSize: '0.9rem'}}>
          {isLogin ? "Don't have an account? " : "Already have an account? "}
          <a 
            href="#!" 
            style={{color: 'var(--primary)', cursor: 'pointer', fontWeight: 500, textDecoration: 'none'}} 
            onClick={() => setIsLogin(!isLogin)}
          >
            {isLogin ? "Register here" : "Login here"}
          </a>
        </div>
      </form>
    </div>
  );
}

export default Auth;
