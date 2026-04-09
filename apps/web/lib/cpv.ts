/** CPV 2-digit prefix → human-readable Latvian category name */
export const CPV_LABELS: Record<string, string> = {
  "03": "Lauksaimniecība",
  "09": "Enerģija",
  "14": "Izrakteņi",
  "15": "Pārtika",
  "22": "Drukātie materiāli",
  "30": "IT tehnika",
  "31": "Elektrotehnika",
  "32": "Radio un sakari",
  "33": "Medicīnas aprīkojums",
  "34": "Transports",
  "35": "Drošība",
  "38": "Laboratorijas iekārtas",
  "39": "Mēbeles",
  "42": "Ražošanas iekārtas",
  "44": "Celtniecības materiāli",
  "45": "Celtniecība",
  "48": "Programmatūra",
  "50": "Uzturēšana un remonts",
  "55": "Ēdināšana",
  "60": "Transports (pakalpojumi)",
  "63": "Loģistika",
  "64": "Sakaru pakalpojumi",
  "65": "Komunālie pakalpojumi",
  "66": "Finanšu pakalpojumi",
  "70": "Nekustamais īpašums",
  "71": "Inženierpakalpojumi",
  "72": "IT pakalpojumi",
  "73": "Pētniecība",
  "75": "Valsts pārvalde",
  "77": "Lauksaimniecības pakalpojumi",
  "79": "Konsultācijas",
  "80": "Izglītība",
  "85": "Veselības aprūpe",
  "90": "Vides pakalpojumi",
  "92": "Kultūra",
  "98": "Citi pakalpojumi",
};

export function cpvLabel(prefix: string): string {
  const key = prefix.length >= 2 ? prefix.slice(0, 2) : prefix;
  return CPV_LABELS[key] ?? `CPV ${key}xxx`;
}
