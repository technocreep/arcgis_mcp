import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import Header from '../components/Header'

const readErrorMessage = async (res) => {
  try {
    const txt = await res.text()
    try {
      const j = txt ? JSON.parse(txt) : null
      return j?.detail || j?.message || txt || `${res.status} ${res.statusText}`
    } catch (e) {
      return txt || `${res.status} ${res.statusText}`
    }
  } catch (e) {
    return `${res.status} ${res.statusText}`
  }
}

const refreshUserInfo = async ({ throwOnError } = { throwOnError: false }) => {
  const res = await fetch('/api/user_info', {
    method: 'GET',
    credentials: 'include',
  })
  if (!res.ok) {
    if (throwOnError) throw new Error(await readErrorMessage(res))
    return null
  }
  return res.json().then((d) => d.user)
}

export default function Login() {
  const [email, setEmail] = useState('')
  const [password, setPassword] = useState('')
  const [status, setStatus] = useState({ kind: 'idle', message: '' })
  const navigate = useNavigate()

  const onSubmit = async (e) => {
    e.preventDefault()
    setStatus({ kind: 'loading', message: 'Logging in...' })
    try {
      const body = new URLSearchParams()
      body.set('username', email)
      body.set('password', password)

      const res = await fetch('/api/token', {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body: body.toString(),
        credentials: 'include',
      })

      if (!res.ok) {
        throw new Error(await readErrorMessage(res))
      }

      // `/token` sets an HttpOnly cookie; we then call `/user_info` to get user data
      await res.text().catch(() => null)
      const me = await refreshUserInfo({ throwOnError: true })
      if (!me) throw new Error('Login succeeded, but /user_info returned empty response')

      setStatus({ kind: 'ok', message: 'Logged in' })
      navigate('/projects')
    } catch (err) {
      setStatus({ kind: 'err', message: String(err?.message || err) })
    }
  }

  return (
    <div>
      <Header onChangeCredentials={() => {}} onUpload={() => alert('Upload not available')} />
      <div className="auth-container">
        <form className="auth-box" onSubmit={onSubmit}>
          <h1>Sign in</h1>
          <label>
            Email
            <input value={email} onChange={(e) => setEmail(e.target.value)} />
          </label>
          <label>
            Password
            <input type="password" value={password} onChange={(e) => setPassword(e.target.value)} />
          </label>
          {status.kind === 'err' && <div className="auth-error">{status.message}</div>}
          <div className="auth-actions">
            <button type="submit">Sign in</button>
          </div>
        </form>
      </div>
    </div>
  )
}
