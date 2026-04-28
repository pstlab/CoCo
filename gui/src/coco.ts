export namespace coco {
  export class CoCo {

    private access_token: string | null = null;
    private readonly classes: Map<string, CoCoClass> = new Map();
    private readonly objects: Map<string, CoCoObject> = new Map();
    private readonly rules: Map<string, CoCoRule> = new Map();
    private socket: WebSocket | null = null;
    private readonly listeners: Set<CoCoListener> = new Set();

    constructor() {
      this.access_token = this.access_token || localStorage.getItem('coco_access_token');
      if (this.access_token) this.connect();
    }

    connect() {
      if (this.socket)
        this.socket.close();

      this.socket = new WebSocket((window.location.protocol === 'https:' ? 'wss' : 'ws') + '://' + window.location.host + '/ws?token=' + encodeURIComponent(this.access_token || ''));
      this.socket.onopen = () => {
        console.log('CoCo connected');
        for (const listener of this.listeners) listener.connected();
      };
      this.socket.onclose = () => {
        console.log('CoCo disconnected');
        for (const listener of this.listeners) listener.disconnected();
      };
      this.socket.onerror = (error) => {
        console.error('CoCo connection error', error);
        for (const listener of this.listeners) listener.connection_error(error);
      };
      this.socket.onmessage = (event) => {
        console.trace('CoCo message received:', event.data);
        const msg: ServerMessage = JSON.parse(event.data);
        switch (msg.msg_type) {
          case 'coco': {
            for (const [name, cls] of Object.entries(msg.classes))
              this.classes.set(name, new CoCoClass(this, name, new Set(cls.parents || []), new Map(Object.entries(cls.static_properties || {})), new Map(Object.entries(cls.dynamic_properties || {}))));
            if (msg.objects)
              for (const [id, obj] of Object.entries(msg.objects))
                this.objects.set(id, new CoCoObject(this, id, new Set(obj.classes.map(cls_name => this.get_class(cls_name))), obj.properties, obj.values));
            for (const listener of this.listeners) listener.initialized();
            break;
          }
          case 'class-created': {
            const cls = new CoCoClass(this, msg.name, new Set(msg.parents || []), new Map(Object.entries(msg.static_properties || {})), new Map(Object.entries(msg.dynamic_properties || {})));
            this.classes.set(cls.get_name(), cls);
            for (const listener of this.listeners) listener.created_class(cls);
            break;
          }
          case 'rule-created': {
            const rule = new CoCoRule(this, msg.name, msg.content);
            this.rules.set(rule.get_name(), rule);
            for (const listener of this.listeners) listener.created_rule(rule);
            break;
          }
          case 'object-created': {
            const obj = new CoCoObject(this, msg.id, new Set(msg.classes.map(cls_name => this.get_class(cls_name))), msg.properties, msg.values);
            this.objects.set(obj.get_id(), obj);
            for (const listener of this.listeners) listener.created_object(obj);
            break;
          }
          case 'added-class': {
            const obj = this.get_object(msg.object_id);
            const cls = this.get_class(msg.class_name);
            obj._add_class(cls);
            break;
          }
          case 'updated-properties': {
            const obj = this.get_object(msg.object_id);
            obj._set_properties(msg.properties);
            break;
          }
          case 'added-values': {
            const obj = this.get_object(msg.object_id);
            obj._set_values(msg.values, msg.date_time);
            break;
          }
        }
      }
    }

    async login(username: string, password: string): Promise<void> {
      const res = await fetch('/login', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ username, password })
      });

      if (!res.ok) {
        throw new Error(`Login failed: ${res.status} ${res.statusText}`.trim());
      }

      const data = await res.json() as { access_token?: string };
      if (!data.access_token) {
        throw new Error('Login failed: missing access token in response');
      }

      this.access_token = data.access_token;
      localStorage.setItem('coco_access_token', this.access_token);
      this.connect();
    }

    logout() {
      console.log('Logging out');
      this.access_token = null;
      localStorage.removeItem('coco_access_token');
      if (this.socket)
        this.socket.close();
    }

    get_access_token(): string | null { return this.access_token; }

    get_classes(): ReadonlyMap<string, CoCoClass> { return this.classes; }
    get_class(name: string): CoCoClass { return this.classes.get(name)!; }

    get_objects(): ReadonlyMap<string, CoCoObject> { return this.objects; }
    get_object(id: string): CoCoObject { return this.objects.get(id)!; }

    get_rules(): ReadonlyMap<string, CoCoRule> { return this.rules; }
    get_rule(name: string): CoCoRule { return this.rules.get(name)!; }

    add_listener(listener: CoCoListener) { this.listeners.add(listener); }
    remove_listener(listener: CoCoListener) { this.listeners.delete(listener); }
  }

  export class CoCoClass {

    private readonly coco: CoCo;
    private readonly name: string;
    private readonly parents: Set<string>;
    private readonly static_properties: Map<string, Property>;
    private readonly dynamic_properties: Map<string, Property>;
    private readonly instances: Set<CoCoObject> = new Set();
    private readonly listeners: Set<CoCoClassListener> = new Set();

    constructor(coco: CoCo, name: string, parents: Set<string> = new Set(), static_properties: Map<string, Property> = new Map(), dynamic_properties: Map<string, Property> = new Map()) {
      this.coco = coco;
      this.name = name;
      this.parents = parents;
      this.static_properties = static_properties;
      this.dynamic_properties = dynamic_properties;
    }

    get_coco(): CoCo { return this.coco; }
    get_name(): string { return this.name; }
    get_parents(): ReadonlySet<string> { return this.parents; }
    get_static_properties(): ReadonlyMap<string, Property> { return this.static_properties; }
    get_dynamic_properties(): ReadonlyMap<string, Property> { return this.dynamic_properties; }
    get_instances(): ReadonlySet<CoCoObject> { return this.instances; }
    _add_instance(obj: CoCoObject) {
      this.instances.add(obj);
      for (const listener of this.listeners) listener.instance_added(obj);
    }

    add_listener(listener: CoCoClassListener) { this.listeners.add(listener); }
    remove_listener(listener: CoCoClassListener) { this.listeners.delete(listener); }
  }

  export class CoCoObject {

    private readonly coco: CoCo;
    private readonly id: string;
    private readonly classes: Set<CoCoClass>;
    private readonly properties: Record<string, Value>;
    private readonly values: Record<string, TimeValue>;
    private data_loaded = false;
    private readonly data: Record<string, Array<TimeValue>> = {};
    private readonly listeners: Set<CoCoObjectListener> = new Set();

    constructor(coco: CoCo, id: string, classes: Set<CoCoClass>, properties?: Record<string, Value>, values?: Record<string, TimeValue>) {
      this.coco = coco;
      this.id = id;
      this.classes = classes;
      this.properties = properties || {};
      this.values = values || {};
      for (const cls of classes) cls._add_instance(this);
    }

    get_coco(): CoCo { return this.coco; }
    get_id(): string { return this.id; }
    get_classes(): ReadonlySet<CoCoClass> { return this.classes; }
    _add_class(cls: CoCoClass) {
      this.classes.add(cls); cls._add_instance(this);
      for (const listener of this.listeners) listener.class_added(cls);
    }
    get_properties(): Record<string, Value> | undefined { return this.properties; }
    _set_properties(properties: Record<string, Value>) {
      for (const [key, value] of Object.entries(properties))
        this.properties![key] = value;
      for (const listener of this.listeners) listener.properties_updated(properties);
    }
    get_values(): Record<string, TimeValue> | undefined { return this.values; }
    _set_values(values: Record<string, Value>, date_time: string) {
      for (const [key, value] of Object.entries(values)) {
        this.values![key] = { value, timestamp: date_time };
        if (!this.data[key]) this.data[key] = [];
        this.data[key].push({ value, timestamp: date_time });
      }
      for (const listener of this.listeners) listener.values_added(values, date_time);
    }

    is_data_loaded(): boolean { return this.data_loaded; }
    get_data(): Record<string, Array<TimeValue>> { return this.data; }
    load_data(from = Date.now() - 1000 * 60 * 60 * 24 * 14, to = Date.now()) {
      const params = new URLSearchParams({
        start: new Date(from).toISOString(),
        end: new Date(to).toISOString()
      });
      fetch(`/objects/${this.id}/data?${params.toString()}`, { headers: { 'Authorization': 'Bearer ' + this.coco.get_access_token() } }).then(res => {
        if (!res.ok) throw new Error(`Failed to load object data: ${res.statusText}`);
        return res.json();
      }).then((data: Record<string, Array<TimeValue>>) => {
        for (const key of Object.keys(this.data))
          delete this.data[key];
        for (const [key, values] of Object.entries(data))
          this.data[key] = values;
        this.data_loaded = true;
        for (const listener of this.listeners) listener.data_updated(data);
      }).catch(error => {
        console.error('Error loading object data:', error);
      });
    }

    add_listener(listener: CoCoObjectListener) { this.listeners.add(listener); }
    remove_listener(listener: CoCoObjectListener) { this.listeners.delete(listener); }
  }

  export class CoCoRule {

    private readonly coco: CoCo;
    private readonly name: string;
    private readonly content: string;

    constructor(coco: CoCo, name: string, content: string) {
      this.coco = coco;
      this.name = name;
      this.content = content;
    }

    get_coco(): CoCo { return this.coco; }
    get_name(): string { return this.name; }
    get_content(): string { return this.content; }
  }

  export interface CoCoListener {

    connected(): void;
    disconnected(): void;
    connection_error(error: Event): void;

    initialized(): void;
    created_class(cls: CoCoClass): void;
    created_object(obj: CoCoObject): void;
    created_rule(rule: CoCoRule): void;
  }

  export interface CoCoClassListener {
    instance_added(obj: CoCoObject): void;
  }

  export interface CoCoObjectListener {
    class_added(cls: CoCoClass): void;
    properties_updated(properties: Record<string, Value>): void;
    values_added(values: Record<string, Value>, date_time: string): void;
    data_updated(data: Record<string, Array<TimeValue>>): void;
  }

  export type Value = null | boolean | number | string;
  export type TimeValue = { value: Value, timestamp: string };

  export function value_to_string(value: Value): string {
    switch (typeof value) {
      case 'string': return value;
      case 'number': return value.toString();
      case 'boolean': return value ? 'true' : 'false';
      default: return '';
    }
  }

  export type Property =
    | { type: 'bool', default?: boolean }
    | { type: 'int', default?: number, min?: number, max?: number }
    | { type: 'float', default?: number, min?: number, max?: number }
    | { type: 'string', default?: string }
    | { type: 'symbol', default?: string, allowed_values?: string[] }
    | { type: 'object', default?: string, class: string };

  type PartialClassMessage = {
    parents?: string[];
    static_properties?: Record<string, Property>;
    dynamic_properties?: Record<string, Property>;
  };
  type ClassMessage = ({ name: string } & PartialClassMessage);

  type PartialObjectMessage = { classes: string[], properties?: Record<string, Value>, values?: Record<string, TimeValue> };
  type ObjectMessage = ({ id: string } & PartialObjectMessage);

  type CoCoMessage = { classes: Record<string, PartialClassMessage>, objects?: Record<string, PartialObjectMessage> };

  type ServerMessage =
    | ({ msg_type: 'coco' } & CoCoMessage)
    | ({ msg_type: 'class-created' } & ClassMessage)
    | ({ msg_type: 'rule-created', name: string, content: string })
    | ({ msg_type: 'object-created' } & ObjectMessage)
    | ({ msg_type: 'added-class', object_id: string, class_name: string })
    | ({ msg_type: 'updated-properties', object_id: string, properties: Record<string, Value> })
    | ({ msg_type: 'added-values', object_id: string, values: Record<string, Value>, date_time: string });
}