import { h, VNode } from "snabbdom";
import { coco } from "../coco";
import { App, flick, Navbar, NavbarItem, NavbarList, OffcanvasBrand } from "@ratiosolver/flick";
import { CoCoOffcanvas } from "./offcanvas";
import { taxonomy } from "./taxonomy";
import { UserButton } from "./user";

const landing_page = () => h('div.container.mt-5', [
  h('header.text-center.mb-5', [
    h('h1.display-4', 'CoCo'),
    h('p.lead.text-body-secondary', 'Combined deduCtiOn and abduCtiOn Reasoner'),
  ]),
  h('div.row.justify-content-center', [
    h('div.col-lg-8', [
      h('p', 'CoCo is a dual-process inspired cognitive architecture built in Rust. It integrates a rule-based expert system and a timeline-based planner to invoke deductive and abductive reasoning in dynamic environments.'),
      h('hr.my-4'),
      h('h4', 'Features'),
      h('ul.list-group.list-group-flush', [
        h('li.list-group-item', [h('i.fas.fa-project-diagram.me-2.text-primary'), h('strong', 'Dual-process Reasoning'), ': Combines a forward-chaining deductive engine with abductive inference to explain observed evidence.']),
        h('li.list-group-item', [h('i.fas.fa-database.me-2.text-primary'), h('strong', 'Dynamic Knowledge Base'), ': Classes and objects evolve at runtime. Properties update continuously and a full time-series history is retained.']),
        h('li.list-group-item', [h('i.fas.fa-cog.me-2.text-primary'), h('strong', 'CLIPS Expert System'), ': Leverages the battle-tested CLIPS rule engine via seamless Rust bindings for expressive, pattern-based rule evaluation.']),
        h('li.list-group-item', [h('i.fas.fa-broadcast-tower.me-2.text-primary'), h('strong', 'Real-time Notifications'), ': Clients subscribe to reasoner events over WebSocket and receive inferences as they fire.']),
      ]),
      h('hr.my-4'),
      h('p', [
        'The knowledge base is organized around ', h('strong', 'classes'), ' and ', h('strong', 'objects'), '. Classes define the structure of things the reasoner knows about. They carry ', h('i', 'static properties'), ' (fixed at creation time) and ', h('i', 'dynamic properties'), ' (updated as the environment evolves), and can inherit from other classes to form a hierarchy. Objects are instances of one or more classes; their properties can be updated at any time, and a full history of their values is recorded as time-series data that can be queried over any time range.',
      ]),
      h('p', [
        'Reasoning is driven by ', h('strong', 'rules'), '. Each rule encodes a piece of domain knowledge: given certain conditions on the current state of objects and their data, the reasoner fires the appropriate conclusions, either deducing new facts or hypothesising explanations for observed evidence. Rules can be added dynamically, allowing the knowledge base to grow and adapt without restarting the system.',
      ]),
      h('p', [
        'Clients can interact with CoCo either through its REST API or by opening a ', h('strong', 'WebSocket connection'), ' to receive real-time notifications whenever the reasoner produces new inferences.',
      ]),
    ])
  ])
]);

const connection_listener = {
  connected: () => { },
  user_updated: () => { },
  disconnected: () => {
    flick.ctx.current_page = landing_page;
    flick.ctx.page_title = 'Home';
  },
  connection_error: (_error: Event) => { },
};

const coco_listener = {
  initialized: () => flick.redraw(),
  created_class: (_cls: coco.CoCoClass) => flick.redraw(),
  created_object: (_obj: coco.CoCoObject) => flick.redraw(),
  created_rule: (_rule: coco.CoCoRule) => flick.redraw(),
};

flick.ctx.current_page = landing_page;
flick.ctx.page_title = 'Home';

export function CoCoApp(coco: coco.CoCo): VNode {
  const content = h('div.flex-grow-1.d-flex.flex-column',
    {
      hook: {
        insert: () => {
          coco.add_connection_listener(connection_listener);
          coco.add_listener(coco_listener);
        },
        destroy: () => {
          coco.remove_connection_listener(connection_listener);
          coco.remove_listener(coco_listener);
        }
      }
    }, [
    (flick.ctx.current_page as () => VNode)(),
    CoCoOffcanvas(coco)
  ]);

  return App(Navbar(OffcanvasBrand('CoCo'), [NavbarList([NavbarItem(h('i.fas.fa-home', {
    on: {
      click: () => {
        flick.ctx.current_page = landing_page;
        flick.ctx.page_title = 'Home';
        flick.redraw();
      }
    }
  })),
  NavbarItem(h('i.fas.fa-sitemap', {
    on: {
      click: () => {
        flick.ctx.current_page = () => taxonomy(coco);
        flick.ctx.page_title = 'Taxonomy';
        flick.redraw();
      }
    }
  }))]), UserButton(coco)]), content);
}