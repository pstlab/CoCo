import { Dropdown, Modal } from "bootstrap";
import { coco } from "../coco";
import { h, VNode } from "snabbdom";

const LOGIN_MODAL_ID = 'coco-login-modal';

export function UserButton(coco: coco.CoCo): VNode {
  let username = "";
  let password = "";

  const showLoginModal = () => {
    const modalEl = document.getElementById(LOGIN_MODAL_ID);
    if (!modalEl) return;
    Modal.getOrCreateInstance(modalEl).show();
  };

  const hideLoginModal = () => {
    const modalEl = document.getElementById(LOGIN_MODAL_ID);
    if (!modalEl) return;
    Modal.getOrCreateInstance(modalEl).hide();
  };

  return h('div.btn-group', [
    h('button.btn.dropdown-toggle', {
      attrs: { 'data-bs-toggle': 'dropdown' },
      hook: {
        insert: (vnode) => {
          const el = vnode.elm as HTMLElement;
          const dropdown = Dropdown.getOrCreateInstance(el);
          el.onclick = () => dropdown.toggle();
        }
      }
    }, h('i.fas.fa-user-circle')),
    h('ul.dropdown-menu.dropdown-menu-end', [
      LoginItem(showLoginModal),
      LogoutItem(coco)
    ]),
    LoginModal(
      () => username,
      (value) => { username = value; },
      () => password,
      (value) => { password = value; },
      async () => {
        if (!username || !password) {
          alert('Username and password are required.');
          return;
        }

        try {
          await coco.login(username, password);
          hideLoginModal();
          username = "";
          password = "";
        } catch (err) {
          const message = err instanceof Error ? err.message : String(err);
          alert(message.startsWith('Login failed:') ? message : 'Login failed: ' + message);
        }
      }
    )
  ]);
}

function LoginItem(openModal: () => void): VNode {
  return h('li', h('button.dropdown-item', {
    on: {
      click: () => {
        openModal();
      }
    }
  }, 'Login'));
}

function LoginModal(getUsername: () => string, setUsername: (value: string) => void, getPassword: () => string, setPassword: (value: string) => void, onSubmit: () => Promise<void>): VNode {
  return h('div.modal.fade', {
    attrs: {
      id: LOGIN_MODAL_ID,
      tabindex: '-1',
      'aria-hidden': 'true'
    }
  }, [
    h('div.modal-dialog', [
      h('div.modal-content', [
        h('div.modal-header', [
          h('h5.modal-title', 'Login'),
          h('button.btn-close', {
            attrs: {
              type: 'button',
              'data-bs-dismiss': 'modal',
              'aria-label': 'Close'
            }
          })
        ]),
        h('div.modal-body', [
          h('div.mb-3', [
            h('label.form-label', {
              attrs: { for: 'coco-login-username' }
            }, 'Username'),
            h('input.form-control', {
              attrs: {
                id: 'coco-login-username',
                type: 'text',
                autocomplete: 'username'
              },
              props: { value: getUsername() },
              on: {
                input: (event: Event) => {
                  setUsername((event.target as HTMLInputElement).value);
                }
              }
            })
          ]),
          h('div.mb-3', [
            h('label.form-label', {
              attrs: { for: 'coco-login-password' }
            }, 'Password'),
            h('input.form-control', {
              attrs: {
                id: 'coco-login-password',
                type: 'password',
                autocomplete: 'current-password'
              },
              props: { value: getPassword() },
              on: {
                input: (event: Event) => {
                  setPassword((event.target as HTMLInputElement).value);
                }
              }
            })
          ])
        ]),
        h('div.modal-footer', [
          h('button.btn.btn-secondary', {
            attrs: {
              type: 'button',
              'data-bs-dismiss': 'modal'
            }
          }, 'Cancel'),
          h('button.btn.btn-primary', {
            attrs: { type: 'button' },
            on: {
              click: () => {
                void onSubmit();
              }
            }
          }, 'Login')
        ])
      ])
    ])
  ]);
}

function LogoutItem(coco: coco.CoCo): VNode {
  return h('li', h('button.dropdown-item', {
    on: {
      click: () => {
        coco.logout();
      }
    }
  }, 'Logout'));
}