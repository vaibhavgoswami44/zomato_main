import { GoogleGenAI } from "@google/genai";
import mongoose from "mongoose";
import axios from "axios";
import * as dotenv from "dotenv";
import * as fs from "fs/promises";

dotenv.config();

// Init Gemini
const ai = new GoogleGenAI({ apiKey: "AIzaSyBuqGZhR9ATigN_wCEx43HFaV77QtXrv8Q" });

// Connect MongoDB
await mongoose.connect(process.env.MONGO_URI, {
  useNewUrlParser: true,
  useUnifiedTopology: true,
});

// Schemas
const zomatoSchema = new mongoose.Schema({}, { strict: false });
const Zomato = mongoose.model("collection_zomato", zomatoSchema, "collection_zomato");
const Scrap = mongoose.model("scrap_data_main", zomatoSchema, "scrap_data_main");

// Fetch Cloudinary image
async function fetchImageBase64(publicId) {
  const url = `https://res.cloudinary.com/${process.env.CLOUD_NAME}/image/upload/zomato/${publicId}.jpg`;
  const res = await axios.get(url, { responseType: "arraybuffer" });
  return Buffer.from(res.data).toString("base64");
}

const promptText = `Extract the menu from this image and return it in valid JSON format only.
Do not use markdown or explanations.

Each item should have the following fields:
- category
- name
- age (if not available, return null)
- size (e.g., "glass", "bottle", "180ml", "500ml", "small", "full", etc.)
- Price (as a number)

If an item is available in multiple sizes (like glass, bottle, ml, or other variants), create separate objects for each with the corresponding size and price.

Return the result as an array of JSON objects like this:
[
  {
    "category": "IMPORTED REDS",
    "name": "AG 47 MALBEC SHIRAZ",
    "age": null,
    "size": "glass",
    "Price": 635
  },
  {
    "category": "IMPORTED REDS",
    "name": "AG 47 MALBEC SHIRAZ",
    "age": null,
    "size": "bottle",
    "Price": 3295
  },
  {
    "category": "BEER",
    "name": "Kingfisher",
    "age": null,
    "size": "500ml",
    "Price": 195
  }
]
`;

// Fetch all pending docs
const docs = await Zomato.find({ status: "pending" });

for (const doc of docs) {
  const objectId = doc._id;

  try {
    // ✅ Mark as "working"
    await Zomato.updateOne({ _id: objectId }, { $set: { status: "working" } });

    // 🔄 Get base64 image
    const base64 = await fetchImageBase64(objectId);

    const contents = [
      {
        inlineData: {
          mimeType: "image/jpeg",
          data: base64,
        },
      },
      { text: promptText },
    ];

    // 💬 Gemini request
    const result = await ai.models.generateContent({
      model: "gemini-2.0-flash",
      contents,
    });

    const text = result?.candidates?.[0]?.content?.parts?.[0]?.text ?? "";
    const cleanText = text.replace(/^```json\s*/, "").replace(/```$/, "").trim();

    let items = JSON.parse(cleanText);

    // 🏷 Add extra fields to every item
    items = items.map(item => ({
      ...item,
      Sr_No: doc.Sr_No,
      Zomato_URL: doc.Zomato_URL,
      Zomato_Menu_Image_URL: doc.Zomato_Menu_Image_URL,
      Location: doc.Location
    }));

    // 📥 Save to new collection
    await Scrap.insertMany(items);

    // ✅ Mark as done
    await Zomato.updateOne({ _id: objectId }, { $set: { status: "done" } });
    console.log(`✅ Done processing ${objectId}`);
  } catch (err) {
    console.error(`❌ Error processing ${objectId}:`, err.message);

    // ❌ Mark as error
    await Zomato.updateOne({ _id: objectId }, { $set: { status: "error" } });
  }
}

console.log("✅ All pending records processed.");
process.exit();
