export type TarHeader={path:string;size:number;type:'file'|'directory'|'link'};
export function mergeMetadata(header:TarHeader,globalPax:Record<string,string>,localPax:Record<string,string>,longname?:string):TarHeader{return {...header,...localPax,...globalPax,path:globalPax.path??localPax.path??longname??header.path,size:Number(localPax.size??globalPax.size??header.size)}}
export class ArchiveIndex{#entries:TarHeader[]=[];add(entry:TarHeader){this.#entries.push(entry)}list(){return this.#entries.slice()}find(path:string){return this.#entries.find(entry=>entry.path===path)}}
