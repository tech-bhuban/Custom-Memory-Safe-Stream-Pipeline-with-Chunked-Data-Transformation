
const express = require('express');
const { Transform, pipeline } = require('stream');
const fs = require('fs');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3000;
const OUTPUT_FILE = path.join(__dirname, 'transformed_output.dat');

// Advanced: Extending the native Transform Stream class (True Senior Pattern)
class UpperCaseCryptoStream extends Transform {
    constructor(options) {
        super(options);
        this.processedBytes = 0;
    }

    // Intercepts and transforms data chunks as they stream through kernel buffers
    _transform(chunk, encoding, callback) {
        try {
            this.processedBytes += chunk.length;
            
            // 1. Convert chunk buffer to string and perform logic
            const transformedData = chunk.toString('utf8').toUpperCase();
            
            // 2. Push the transformed binary data back into the pipeline
            this.push(Buffer.from(transformedData));
            
            // 3. Signal that this chunk is complete and ready for the next one
            callback();
        } catch (err) {
            callback(err); // Propagate stream error safely
        }
    }

    _flush(callback) {
        console.log(`[Stream Pipeline] Complete. Total chunk bytes processed: ${this.processedBytes}`);
        callback();
    }
}

// Ingestion endpoint handling huge data files safely via data piping
app.post('/api/stream-transform', (req, res) => {
    console.log('[API Gateway] Initiating memory-safe file transform stream pipeline...');

    const transformer = new UpperCaseCryptoStream();
    const writeStream = fs.createWriteStream(OUTPUT_FILE);

    // Advanced: Using pipeline() to handle backpressure and error cleanup automatically
    pipeline(
        req,          // Readable Source Stream (Incoming HTTP Request)
        transformer,  // Transform Stream Middleware (Our class logic)
        writeStream,  // Writable Target Stream (Local disk file)
        (err) => {
            if (err) {
                console.error(`❌ [Pipeline Fault] Stream broke down: ${err.message}`);
                // Safely clean up file descriptors
                if (fs.existsSync(OUTPUT_FILE)) fs.unlinkSync(OUTPUT_FILE);
                return res.status(500).json({ success: false, error: 'Data pipeline integration error' });
            }
            
            res.status(201).json({
                success: true,
                message: 'Stream successfully processed and drained to storage disk.',
                outputAsset: 'transformed_output.dat'
            });
        }
    );
});

// Admin diagnostic route to watch active system properties
app.get('/admin/pipeline-metrics', (req, res) => {
    res.json({
        outputFileExists: fs.existsSync(OUTPUT_FILE),
        outputFileSize: fs.existsSync(OUTPUT_FILE) ? `${fs.statSync(OUTPUT_FILE).size} Bytes` : '0 Bytes',
        memoryUsage: process.memoryUsage().heapUsed,
        pid: process.pid
    });
});

app.listen(PORT, () => {
    console.log(`🚀 Stream Data Node running on port ${PORT}`);
    console.log(`Pipeline sink configured at: ${OUTPUT_FILE}`);
});

// # ⚡ High-Performance Memory-Safe Data Transformation Pipeline

// A low-overhead backend architecture utility designed to manipulate massive unstructured data payloads seamlessly via Node.js native streams. By handling requests as progressive binary chunks, the server completely avoids RAM allocation spikes.

// ## 🛠 Advanced Features
// - **Custom Transform Architecture**: Inherits directly from the native Node.js `Transform` stream class to manipulate incoming binary buffers on-the-fly.
// - **Backpressure Safety Controls**: Utilizes the modern `pipeline()` structure to coordinate data transmission rates between reading and writing channels automatically, preventing memory overflow.
// - **Constant Memory Footprint**: Processes files of any size (from 1KB to 10GB+) using fixed block segments, keeping V8 engine heap consumption down.
// - **Fail-Soft Handle Teardowns**: Intercepts pipeline breakdowns mid-execution, wiping partial corrupted assets from the local filesystem during a crash.

// ## 🚀 Setup & Verification
// 1. **Initialize Project Environment**:
//    ```bash
//    npm install express
//    ```
// 2. **Start Pipeline Node**:
//    ```bash
//    node server.js
//    ```
// 3. **Stream a Mock Data Payload**:
//    Send a raw text string payload directly through the transformation pipe:
//    ```bash
//    curl -X POST http://localhost:3000/api/stream-transform \
//      -H "Content-Type: text/plain" \
//      -d "streaming raw data inputs through custom node native structures."
//    ```
// 4. **Audit Local Asset Output**:
//    Check your root folder for the generated `transformed_output.dat` file. It will contain your completely capitalized data structure.

// ## ⚙️ Engineering Principles Behind the Code
// Standard applications load client payloads completely into memory strings before processing them (`express.json()`). This method introduces massive vulnerability vectors under high load or malicious large file uploads. Shifting to an explicit, data-draining stream parser creates an incredibly robust ecosystem optimized for cloud deployments where hardware container RAM bounds must remain tight and predictable.

// ## License
// MIT
