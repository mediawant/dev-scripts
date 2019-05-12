const fs = require('fs');
const _ = require('lodash');
const cheerio = require('cheerio');
const csv = require('fast-csv');

const stream = fs.createReadStream("./data/youtube_raw_data-190512.csv");
let writableStream = fs.createWriteStream("./results/youtube-humans-data.csv");

let csvStream = csv.createWriteStream({headers: true});

let count = 0;

writableStream.on("finish", () => {
    csvStream.end();
});

csvStream.pipe(writableStream);

try {
    csv.fromStream(stream, {trim: true})
        .validate((data) => {
            return data[0].indexOf('?v=') !== -1
        })
        .on("data-invalid", (line) => {
            // console.log("Invalid: " + line[0]);
        })
        .on("data", (line) => {
            const $ = cheerio.load(_.get(line, '44'));

            const videoId = _.get(line, '0').split('&')[0].split('?v=').pop();
            const title = _.get(line, '5').replace(' - YouTube', '');
            const description = _.get(line, '8');
            const keywords = _.get(line, '11');
            const viewCount = _.get(line, '41').replace(' lượt xem', '');
            const uploadDate = _.get(line, '43').replace('Xuất bản ', '')
                .replace('Đã công chiếu vào ', '')
                .replace('Đã công chiếu ', '')
                .replace('Đã phát trực tiếp ', '')
                .replace('Đã phát trực tuyến vào ', '')
                .replace('Đã tải lên vào ', '');

            const channelName = $('a').text();
            const channelId = _.replace($('a').attr('href'), '/channel/', '');

            csvStream.write({
                video_id: videoId,
                title: title,
                description: description,
                keywords: keywords,
                view_count: viewCount,
                upload_date: uploadDate,
                channel_name: channelName,
                channel_id: channelId
            });

            count += 1;
            console.log(`${count}: ${title}`);
        })
        .on("end", () => {
            console.log("read done!");
        });
} catch (e) {
    console.log(e);
}
