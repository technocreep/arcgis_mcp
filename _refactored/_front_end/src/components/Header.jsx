import React from 'react'
import "./Header.css"
import icon from  "../assets/overlay.png"

class Header extends React.Component {
  renderUserLabel(user) {
    if (!user) return null
    if (typeof user === 'string') return user
    if (user.username) return user.username
    if (user.name) return user.name
    if (user.user_id || user.userId || user.id) return String(user.user_id ?? user.userId ?? user.id)
    try {
      return JSON.stringify(user)
    } catch (e) {
      return String(user)
    }
  }

  render() {
    const {
      user = null,
      onUpload = () => {},
      onChangeCredentials = () => {},
      uploadLabel = 'Upload Project',
      uploadEnabled = true,
    } = this.props

    return (
      <div className="header_body">
          <div className='spacer'></div>
          <div className="label-group">
            <img src={icon} className="brand-icon" alt="Overlay Icon" />
            <h1 className="brand-title">GIS Data <span className="brand-accent">Portal</span></h1>
          </div>
          <div className="status-group">
            {user && <div className="user-label">Signed in as <span className="mono">{this.renderUserLabel(user)}</span></div>}
          </div>
          <div className="button-group">
            <button className={`upload_btn ${!uploadEnabled ? 'disabled' : ''}`} onClick={uploadEnabled ? onUpload : undefined} disabled={!uploadEnabled}>
                <i className="fa-solid fa-plus" style={{ marginRight: 8 }}></i>
                + {uploadLabel}
              </button>
          </div>
          <div className='spacer'></div>
      </div>
    )
  }
}

export default Header
