import { Dropdown } from "bootstrap";
import { coco } from "../coco";
import { h, VNode } from "snabbdom";

export function UserButton(coco: coco.CoCo): VNode {
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
      LoginItem(coco),
      LogoutItem(coco)
    ])
  ]);
}

function LoginItem(coco: coco.CoCo): VNode {
  return h('li', h('button.dropdown-item', {
    on: {
      click: () => {
        const username = prompt('Username:');
        const password = prompt('Password:');
        if (username && password) {
          coco.login(username, password).then(() => {
            console.log('Login successful');
          }).catch(err => {
            alert('Login failed: ' + err.message);
          });
        }
      }
    }
  }, 'Login'));
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