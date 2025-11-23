"""Test different OpenAI models for email generation and compare costs."""
import os
import json
from datetime import datetime
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# Pricing per 1M tokens (as of latest pricing)
PRICING = {
    "gpt-4.1": {"input": 10.00, "output": 30.00},  # GPT-4 Turbo pricing
    "gpt-5.1": {"input": 15.00, "output": 60.00},  # Estimated GPT-5 pricing
    "gpt-5-mini": {"input": 3.00, "output": 12.00},  # Estimated GPT-5-mini pricing
}

# Sample meeting data (condensed from real data)
SAMPLE_PROMPT = """You are UNKNOWN's analyst. Create an executive summary for this week's client meetings.

Data:
- 4 qualified client meetings
- Average score: 4.8/5
- Team: Sam (3 meetings), Ellie (1 meeting)

Key meetings:
1. Footballco - Studio acquisition search (Sam, Score: 5)
2. Instacart - Freelance-to-perm hiring (Ellie, Score: 5)
3. Crispin - Creative agency talent search (Sam, Score: 5)
4. We Are Social US - Senior hire discussion (Sam, Score: 4)

Write a concise 2-3 paragraph executive summary highlighting:
- Overall performance metrics
- Top performer and their key achievement
- Main hiring trends observed"""


def test_gpt4_chat(prompt: str) -> dict:
    """Test GPT-4.1 using Chat Completions API."""
    print("\n" + "="*60)
    print("Testing GPT-4.1 (Chat Completions API)")
    print("="*60)

    response = client.chat.completions.create(
        model="gpt-4-turbo-2024-04-09",  # GPT-4.1 equivalent
        messages=[
            {"role": "system", "content": "You are UNKNOWN's internal analyst."},
            {"role": "user", "content": prompt}
        ],
        temperature=0.3
    )

    content = response.choices[0].message.content
    input_tokens = response.usage.prompt_tokens
    output_tokens = response.usage.completion_tokens

    input_cost = (input_tokens / 1_000_000) * PRICING["gpt-4.1"]["input"]
    output_cost = (output_tokens / 1_000_000) * PRICING["gpt-4.1"]["output"]
    total_cost = input_cost + output_cost

    return {
        "model": "gpt-4.1",
        "content": content,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "total_tokens": input_tokens + output_tokens,
        "input_cost": input_cost,
        "output_cost": output_cost,
        "total_cost": total_cost
    }


def test_gpt5_responses(model: str, prompt: str) -> dict:
    """Test GPT-5 models using Responses API."""
    print("\n" + "="*60)
    print(f"Testing {model} (Responses API)")
    print("="*60)

    # GPT-5.1 supports "none", but GPT-5-mini needs "minimal"
    effort = "none" if model == "gpt-5.1" else "minimal"

    response = client.responses.create(
        model=model,
        input=prompt,
        reasoning={"effort": effort},
        text={"verbosity": "medium"}
    )

    content = response.output_text
    input_tokens = response.usage.input_tokens
    output_tokens = response.usage.output_tokens

    model_key = model
    input_cost = (input_tokens / 1_000_000) * PRICING[model_key]["input"]
    output_cost = (output_tokens / 1_000_000) * PRICING[model_key]["output"]
    total_cost = input_cost + output_cost

    return {
        "model": model,
        "content": content,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "total_tokens": input_tokens + output_tokens,
        "input_cost": input_cost,
        "output_cost": output_cost,
        "total_cost": total_cost
    }


def print_results(result: dict):
    """Print formatted results."""
    print(f"\n📊 Model: {result['model']}")
    print(f"📝 Tokens: {result['input_tokens']:,} in / {result['output_tokens']:,} out = {result['total_tokens']:,} total")
    print(f"💰 Cost: ${result['input_cost']:.6f} in + ${result['output_cost']:.6f} out = ${result['total_cost']:.6f} total")
    print(f"\n📄 Output:\n{result['content']}")


def compare_costs(results: list):
    """Compare costs across models."""
    print("\n" + "="*60)
    print("COST COMPARISON")
    print("="*60)

    print("\nPer Email Generation:")
    for r in results:
        print(f"  {r['model']:15} ${r['total_cost']:.6f}")

    print("\nEstimated Weekly Cost (1 email):")
    for r in results:
        print(f"  {r['model']:15} ${r['total_cost']:.6f}")

    print("\nEstimated Monthly Cost (4 emails):")
    for r in results:
        monthly = r['total_cost'] * 4
        print(f"  {r['model']:15} ${monthly:.6f}")

    print("\nEstimated Yearly Cost (52 emails):")
    for r in results:
        yearly = r['total_cost'] * 52
        print(f"  {r['model']:15} ${yearly:.4f}")

    # Show savings
    baseline = next(r for r in results if r['model'] == 'gpt-4.1')
    print("\nSavings vs GPT-4.1 (per year):")
    for r in results:
        if r['model'] != 'gpt-4.1':
            savings = (baseline['total_cost'] - r['total_cost']) * 52
            pct = ((baseline['total_cost'] - r['total_cost']) / baseline['total_cost']) * 100
            sign = "💰 SAVE" if savings > 0 else "💸 COST"
            print(f"  {r['model']:15} {sign} ${abs(savings):.4f} ({pct:+.1f}%)")


if __name__ == "__main__":
    results = []

    # Test GPT-4.1
    try:
        result = test_gpt4_chat(SAMPLE_PROMPT)
        print_results(result)
        results.append(result)
    except Exception as e:
        print(f"❌ GPT-4.1 failed: {e}")

    # Test GPT-5-mini
    try:
        result = test_gpt5_responses("gpt-5-mini", SAMPLE_PROMPT)
        print_results(result)
        results.append(result)
    except Exception as e:
        print(f"❌ GPT-5-mini failed: {e}")

    # Test GPT-5.1
    try:
        result = test_gpt5_responses("gpt-5.1", SAMPLE_PROMPT)
        print_results(result)
        results.append(result)
    except Exception as e:
        print(f"❌ GPT-5.1 failed: {e}")

    # Compare
    if len(results) >= 2:
        compare_costs(results)

    print("\n" + "="*60)
    print("✅ Test complete!")
    print("="*60)
