import { h, VNode } from "snabbdom";
import { coco } from "../coco";
import { Dropdown, Modal } from "bootstrap";
import { flick } from "@ratiosolver/flick";

let username = "";
let password = "";
let connected = false;

const app_listener = {
  initialized: () => { },
  created_class: (_cls: coco.CoCoClass) => { },
  created_object: (_obj: coco.CoCoObject) => { },
  created_rule: (_rule: coco.CoCoRule) => { },
  connection_error: (_error: Event) => { },
  connected: () => { connected = true; },
  disconnected: () => { connected = false; },
};

export function UserButton(coco: coco.CoCo): VNode {
  let loginModal: Modal | null = null;
  let dropdown: Dropdown | null = null;

  return h('div.btn-group', {
    hook: {
      insert: () => {
        coco.add_listener(app_listener);
      },
      destroy: () => {
        coco.remove_listener(app_listener);
      }
    }
  }, [
    h('button.btn.dropdown-toggle', {
      attrs: { 'data-bs-toggle': 'dropdown' },
      hook: {
        insert: (vnode) => { dropdown = Dropdown.getOrCreateInstance(vnode.elm as Element); },
        update: (_old, vnode) => { dropdown = Dropdown.getOrCreateInstance(vnode.elm as Element); },
        destroy: (_vnode) => {
          dropdown?.dispose();
          dropdown = null;
        }
      },
      on: { click: () => dropdown?.toggle() }
    }, h('i.fas.fa-user-circle')),
    h('ul.dropdown-menu.dropdown-menu-end', [
      connected ?
        h('li', h('button.dropdown-item', {
          on: {
            click: () => {
              coco.logout();
              flick.redraw();
            }
          }
        }, 'Logout')) : h('li', h('button.dropdown-item', {
          on: {
            click: () => {
              loginModal!.show();
            }
          }
        }, 'Login'))
    ]),
    h('div.modal.fade', {
      hook: {
        insert: (vnode) => {
          const modalElement = vnode.elm as Element;
          loginModal = new Modal(modalElement);
          // Restore focus to the user button when modal is hidden
          modalElement.addEventListener('hide.bs.modal', () => {
            const userButton = document.querySelector('.btn-group .btn.dropdown-toggle') as HTMLButtonElement;
            if (userButton) {
              setTimeout(() => userButton.focus(), 0);
            }
          });
        },
        update: (_old, vnode) => { loginModal = new Modal(vnode.elm as Element); },
        destroy: (_vnode) => {
          loginModal?.dispose();
          loginModal = null;
        }
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
            h('form', [
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
                  on: {
                    input: (event: Event) => {
                      username = (event.target as HTMLInputElement).value;
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
                  on: {
                    input: (event: Event) => {
                      password = (event.target as HTMLInputElement).value;
                    }
                  }
                })
              ])
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
                  coco.login(username, password).then(() => {
                    loginModal!.hide();
                    flick.redraw();
                  }).catch((error) => alert(`Login failed: ${error}`));
                }
              }
            }, 'Login')
          ])
        ])
      ])])
  ]);
}