import { h, VNode } from "snabbdom";
import { coco } from "../coco";
import { flick, ListGroup, ListGroupItem } from "@ratiosolver/flick";
import hljs from 'highlight.js/lib/core';
import 'highlight.js/styles/github.css';

hljs.registerLanguage('clips', (hljs) => ({
  name: 'CLIPS',
  case_insensitive: true,
  keywords: {
    $pattern: '[A-Za-z_][\\w$*-]*',
    keyword: [
      'defrule', 'deftemplate', 'defglobal', 'deffacts', 'deffunction', 'defmethod', 'if', 'then', 'else', 'and', 'or', 'not', 'test', 'assert', 'retract', 'modify', 'do-for-fact', 'do-for-all-facts', 'any-factp', 'find-fact', 'find-all-facts', 'create$'
    ].join(' '),
    built_in: [
      'printout', 'println', 'add-data'
    ].join(' '),
    literal: 'TRUE FALSE nil crlf t'
  },
  contains: [
    hljs.COMMENT(';', '$'),
    hljs.QUOTE_STRING_MODE,
    {
      className: 'variable',
      begin: /\$?\?[\w-]+/
    },
    {
      className: 'operator',
      begin: /<=|>=|<>|=|<|>/
    },
    hljs.NUMBER_MODE
  ]
}));

export function RulesList(coco: coco.CoCo): VNode {
  return ListGroup(Array.from(coco.get_rules().values().map(rule => ListGroupItem(rule.get_name(), () => {
    flick.ctx.current_page = () => CoCoRule(rule);
    flick.ctx.page_title = `Rule: ${rule.get_name()}`;
    flick.redraw();
  }, flick.ctx.page_title === `Rule: ${rule.get_name()}`))));
}

export function CoCoRule(rule: coco.CoCoRule): VNode {
  const highlighted = hljs.highlight(rule.get_content(), { language: 'clips' }).value;

  const content = h('div.container.mt-2', [
    h('div.input-group', [
      h('input.form-control', { attrs: { type: 'text', value: rule.get_name(), placeholder: 'Rule name', disabled: true } }),
      h('button.btn.btn-outline-secondary', {
        attrs: { type: 'button', title: 'Copy rule name to clipboard' },
        on: { click: () => navigator.clipboard.writeText(rule.get_name()) }
      }, h('i.fa-solid.fa-copy')),
    ]),
    h('pre.mt-2.p-3.rounded', { style: { background: '#f6f8fa', overflowX: 'auto' } },
      h('code.hljs.language-clips', { props: { innerHTML: highlighted } })
    ),
  ]);
  return content;
}