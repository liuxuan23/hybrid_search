---
license: apache-2.0
task_categories:
- graph-ml
---

# HUggingKG

<!-- Provide a quick summary of the dataset. -->

This dataset contains a large-scale ML resource KG built on the Hugging Face community.

## Dataset Details

- `triples.txt` contains the triple information of the complete graph, in the following form.
    ```
    JeffreyXiang/TRELLIS	space_use_model	JeffreyXiang/TRELLIS-image-large
    black-forest-labs/FLUX.1-dev	space_use_model	black-forest-labs/FLUX.1-dev
    black-forest-labs/FLUX.1-dev	space_use_model	madebyollin/taef1
    Changg/ori	model_definedFor_task	text-to-image
    DmitryYarov/aristotle_based_on_rugpt3large_based_on_gpt	model_definedFor_task	text-generation
    JINJIN7987/llama3-8b-neg-sleeper	model_definedFor_task	text-generation
    ShuhongZheng/sdxl_cat2_w_preserve	model_definedFor_task	text-to-image
    ...
    ```
- `HuggingKG_V20241215174821.zip` contains `json` files of various nodes and edges. Each `json` file is a list of `dict`, where each element consists of attributes of a node/edge.


<table>
  <thead>
  </thead>
  <tbody>
    <tr>
      <td>💻 <strong>GitHub</strong></td>
      <td><a href="https://github.com/nju-websoft/HuggingBench">Code Reposity</a></td>
    </tr>
    <tr>
      <td>📄 <strong>Paper</strong></td>
      <td><a href="https://arxiv.org/abs/2505.17507">ArXiv-Link</a></td>
    </tr>
    <tr>
      <td>📊 <strong>Data</strong></td>
      <td><a href="https://huggingface.co/collections/cqsss/huggingbench-67b2ee02ca45b15e351009a2">
HuggingBench</a></td>
    </tr>
  </tbody>
</table>