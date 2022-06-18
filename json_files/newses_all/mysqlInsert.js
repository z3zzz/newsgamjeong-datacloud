import mysql from "mysql2/promise";
import bluebird from "bluebird";
import prompt from "password-prompt";
import fs from "fs/promises";

const pw = await prompt("password: ");

const conn = await mysql.createConnection({
  host: "10.178.0.5",
  user: "kwang",
  password: pw,
  database: "news",
  Promise: bluebird,
});

for (let i = 1; i < 15; i++) {
  const raw = await fs.readFile("predict_1.json", "utf-8");
  const dataStrings = raw.split(/\r?\n/);

  let normalCount = 0;
  let errorCount = 0;

  for (let dataString of dataStrings) {
    let data;
    try {
      data = JSON.parse(dataString);
    } catch (e) {
      console.log(dataString);
      console.log(e);
      continue;
    }
    const title = "'" + data.text_headline + "'";
    const title2 = '"' + title.slice(1, -1) + '"';
    const title3 = '"' + data.text_headline.replace(/["']/g, "") + '"';
    const company = "'" + data.text_company + "'";
    const category = "'" + data.category + "'";
    const date = "'" + data.time.slice(0, 10) + "'";

    try {
      await conn.execute(
        `insert into newslist (title, company, category, date) values (${title}, ${company}, ${category}, ${date})`
      );
      normalCount += 1;
    } catch (e) {
      try {
        await conn.execute(
          `insert into newslist (title, company, category, date) values (${title2}, ${company}, ${category}, ${date})`
        );
        normalCount += 1;
      } catch (e) {
        try {
          await conn.execute(
            `insert into newslist (title, company, category, date) values (${title3}, ${company}, ${category}, ${date})`
          );
          normalCount += 1;
        } catch (e) {
          console.log(
            `insert into newslist (title, company, category, date) values (${data.text_headline}, ${company}, ${category}, ${date})`
          );
          console.log(`error: ${e}`);
          errorCount += 1;
        }
      }
    }
  }

  console.log(`${i}/14...`);
  console.log({ normalCount });
  console.log({ errorCount });
}
