import { h, VNode } from "snabbdom";
import { coco } from "../coco";
import { flick, Header, ListGroup, ListGroupItem, Row, Table } from "@ratiosolver/flick";
import { CoCoObject } from "./object";

export function ClassesList(coco: coco.CoCo): VNode {
  const classes = coco.get_classes().values().toArray().sort((a, b) => a.get_name().localeCompare(b.get_name()));
  return ListGroup(classes.map(cls => ListGroupItem(cls.get_name(), () => {
    flick.ctx.current_page = () => CoCoClass(cls);
    flick.ctx.page_title = `Class: ${cls.get_name()}`;
    flick.redraw();
  }, flick.ctx.page_title === `Class: ${cls.get_name()}`)));
}

const cls_listener: coco.CoCoClassListener = {
  instance_added: (_obj: coco.CoCoObject) => {
    flick.redraw();
  }
};

const obj_item_listener: coco.CoCoObjectListener = {
  classes_updated: (_classes: Set<coco.CoCoClass>) => { },
  properties_updated: (_properties: Record<string, coco.Value>) => { flick.redraw(); },
  values_added: (_values: Record<string, coco.Value>, _date_time: string) => { flick.redraw(); },
  data_updated: (_data: Record<string, Array<coco.TimeValue>>) => { }
};

function PropertyHeader(): VNode {
  return h('tr', [
    h('th', 'Name'),
    h('th', { style: { width: '6rem', minWidth: '6rem' } }, 'Type'),
    h('th', { style: { width: '2.5rem', minWidth: '2.5rem' } }, 'Info')
  ]);
}

export function ObjectRow(cls: coco.CoCoClass, obj: coco.CoCoObject): VNode {
  const cells = [obj.get_id()];
  for (const prop of cls.get_static_properties().keys().toArray().sort()) {
    const props = obj.get_properties();
    if (props && prop in props)
      cells.push(coco.value_to_string(props[prop]));
    else
      cells.push('');
  }
  for (const prop of cls.get_dynamic_properties().keys().toArray().sort()) {
    const props = obj.get_values();
    if (props && prop in props)
      cells.push(coco.value_to_string(props[prop].value));
    else
      cells.push('');
  }
  return h('tr', {
    hook: {
      insert: () => {
        obj.add_listener(obj_item_listener);
      },
      destroy: () => {
        obj.remove_listener(obj_item_listener);
      }
    },
    style: { cursor: 'pointer' },
    on: {
      click: () => {
        flick.ctx.current_page = () => CoCoObject(obj);
        flick.ctx.page_title = `Object: ${obj.get_id()}`;
        flick.redraw();
      }
    }
  }, cells.map(cell => h('td', cell)));
}

export function CoCoClass(cls: coco.CoCoClass): VNode {
  const parents = cls.get_parents().values().toArray().sort();

  const description_cell = (description?: string): string | VNode => {
    const text = description?.trim();
    if (!text) return '';
    return h('span', { attrs: { title: text } }, h('i.fa-solid.fa-circle-info'));
  };

  const static_props_rows = cls.get_static_properties().entries().toArray().sort(([nameA], [nameB]) => nameA.localeCompare(nameB)).map(([name, type]) => Row([name, type.type, description_cell(type.description)]));
  const dynamic_props_rows = cls.get_dynamic_properties().entries().toArray().sort(([nameA], [nameB]) => nameA.localeCompare(nameB)).map(([name, type]) => Row([name, type.type, description_cell(type.description)]));

  const header = ["ID", ...cls.get_static_properties().keys().toArray().sort(), ...cls.get_dynamic_properties().keys().toArray().sort()];
  const rows = cls.get_instances().values().map(obj => ObjectRow(cls, obj)).toArray();

  return h('div.container.mt-2',
    {
      hook: {
        insert: () => {
          cls.add_listener(cls_listener);
        },
        destroy: () => {
          cls.remove_listener(cls_listener);
        }
      }
    }, [
    h('div.input-group', [
      h('input.form-control', { attrs: { type: 'text', value: cls.get_name(), placeholder: 'Type name', disabled: true } }),
      h('button.btn.btn-outline-secondary', {
        attrs: { type: 'button', title: 'Copy type name to clipboard' },
        on: { click: () => navigator.clipboard.writeText(cls.get_name()) }
      }, h('i.fa-solid.fa-copy')),
    ]),
    parents.length > 0 ?
      h('div.mt-2', parents.map(par_cls_name =>
        h('span.badge.bg-primary.me-1', {
          style: { cursor: 'pointer' },
          on: {
            click: () => {
              flick.ctx.current_page = () => CoCoClass(cls.get_coco().get_class(par_cls_name)!);
              flick.ctx.page_title = `Class: ${par_cls_name}`;
              flick.redraw();
            }
          }
        }, par_cls_name)
      )) : h('p.mt-2', 'No parent classes.'),
    static_props_rows.length > 0 ? Table(PropertyHeader(), static_props_rows, 'Static Properties') : h('p.mt-2', 'No static properties.'),
    dynamic_props_rows.length > 0 ? Table(PropertyHeader(), dynamic_props_rows, 'Dynamic Properties') : h('p.mt-2', 'No dynamic properties.'),
    rows.length > 0 ? Table(Header(header), rows, 'Instances') : h('p.mt-2', 'No instances of this class yet.')
  ]);
}