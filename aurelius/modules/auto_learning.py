"""
Auto-learning module for AURELIUS backend system.
Analyzes patterns and optimizes content and sales strategies automatically.
"""

import asyncio
import json
import statistics
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
from collections import defaultdict, Counter
import re

from ..logger import get_logger, LogOperation, log_async_function_calls
from ..config import settings
from .data_store import data_store, interactions_manager, leads_manager, sales_manager
from .ai import content_generator

logger = get_logger("auto_learning")

class ContentAnalyzer:
    """Analyzes content performance and identifies patterns."""
    
    @log_async_function_calls("auto_learning")
    async def analyze_content_performance(self, days: int = 30) -> Dict[str, Any]:
        """
        Analyze content performance across platforms.
        
        Args:
            days: Number of days to analyze
            
        Returns:
            Content performance analysis
        """
        try:
            # Get recent interactions
            recent_interactions = await interactions_manager.get_recent_interactions(days * 24)
            
            # Analyze content patterns
            content_analysis = {
                "high_performing_content": [],
                "low_performing_content": [],
                "optimal_posting_times": {},
                "effective_hashtags": [],
                "engagement_patterns": {},
                "content_themes": {}
            }
            
            # Group interactions by content
            content_groups = defaultdict(list)
            
            for interaction in recent_interactions:
                content = interaction.get("text", "") or interaction.get("content", "")
                if content and interaction.get("success"):
                    content_groups[content].append(interaction)
            
            # Analyze performance
            content_performance = []
            
            for content, interactions in content_groups.items():
                if len(interactions) >= 2:  # Only analyze content with multiple interactions
                    engagement_score = self._calculate_engagement_score(interactions)
                    
                    content_performance.append({
                        "content": content[:100] + "..." if len(content) > 100 else content,
                        "interactions_count": len(interactions),
                        "engagement_score": engagement_score,
                        "platforms": list(set(i.get("platform", "") for i in interactions)),
                        "avg_response_time": self._calculate_avg_response_time(interactions)
                    })
            
            # Sort by engagement score
            content_performance.sort(key=lambda x: x["engagement_score"], reverse=True)
            
            # Identify high and low performing content
            if content_performance:
                top_quartile = len(content_performance) // 4
                content_analysis["high_performing_content"] = content_performance[:top_quartile or 1]
                content_analysis["low_performing_content"] = content_performance[-top_quartile or 1:]
            
            # Analyze posting times
            content_analysis["optimal_posting_times"] = await self._analyze_posting_times(recent_interactions)
            
            # Analyze hashtags and keywords
            content_analysis["effective_hashtags"] = await self._analyze_hashtags(recent_interactions)
            
            # Analyze engagement patterns
            content_analysis["engagement_patterns"] = await self._analyze_engagement_patterns(recent_interactions)
            
            # Analyze content themes
            content_analysis["content_themes"] = await self._analyze_content_themes(recent_interactions)
            
            return content_analysis
            
        except Exception as e:
            logger.error(f"Error analyzing content performance: {str(e)}")
            return {}
    
    def _calculate_engagement_score(self, interactions: List[Dict[str, Any]]) -> float:
        """Calculate engagement score for content."""
        try:
            score = 0.0
            
            for interaction in interactions:
                # Base score for successful interaction
                if interaction.get("success"):
                    score += 10
                
                # Bonus for specific interaction types
                interaction_type = interaction.get("type", "")
                if interaction_type in ["reply", "mention"]:
                    score += 5
                elif interaction_type in ["like", "favourite"]:
                    score += 3
                elif interaction_type in ["share", "boost"]:
                    score += 8
            
            return score / len(interactions) if interactions else 0
            
        except Exception as e:
            logger.error(f"Error calculating engagement score: {str(e)}")
            return 0.0
    
    def _calculate_avg_response_time(self, interactions: List[Dict[str, Any]]) -> float:
        """Calculate average response time for interactions."""
        try:
            response_times = []
            
            for interaction in interactions:
                timestamp = interaction.get("timestamp")
                if timestamp:
                    try:
                        interaction_time = datetime.fromisoformat(timestamp)
                        # Assume response time is time since post (simplified)
                        response_time = (datetime.now() - interaction_time).total_seconds() / 3600
                        response_times.append(response_time)
                    except:
                        continue
            
            return statistics.mean(response_times) if response_times else 0
            
        except Exception as e:
            logger.error(f"Error calculating average response time: {str(e)}")
            return 0.0
    
    async def _analyze_posting_times(self, interactions: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze optimal posting times."""
        try:
            hourly_engagement = defaultdict(list)
            daily_engagement = defaultdict(list)
            
            for interaction in interactions:
                if interaction.get("success") and interaction.get("timestamp"):
                    try:
                        timestamp = datetime.fromisoformat(interaction["timestamp"])
                        hour = timestamp.hour
                        day = timestamp.strftime("%A")
                        
                        engagement_score = 1 if interaction.get("success") else 0
                        hourly_engagement[hour].append(engagement_score)
                        daily_engagement[day].append(engagement_score)
                    except:
                        continue
            
            # Calculate average engagement by hour and day
            optimal_hours = {}
            for hour, scores in hourly_engagement.items():
                optimal_hours[hour] = statistics.mean(scores) if scores else 0
            
            optimal_days = {}
            for day, scores in daily_engagement.items():
                optimal_days[day] = statistics.mean(scores) if scores else 0
            
            # Find best times
            best_hour = max(optimal_hours.items(), key=lambda x: x[1])[0] if optimal_hours else 12
            best_day = max(optimal_days.items(), key=lambda x: x[1])[0] if optimal_days else "Monday"
            
            return {
                "best_hour": best_hour,
                "best_day": best_day,
                "hourly_performance": optimal_hours,
                "daily_performance": optimal_days
            }
            
        except Exception as e:
            logger.error(f"Error analyzing posting times: {str(e)}")
            return {}
    
    async def _analyze_hashtags(self, interactions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Analyze effective hashtags."""
        try:
            hashtag_performance = defaultdict(list)
            
            for interaction in interactions:
                content = interaction.get("text", "") or interaction.get("content", "")
                if content and interaction.get("success"):
                    # Extract hashtags
                    hashtags = re.findall(r'#\w+', content.lower())
                    engagement_score = 1 if interaction.get("success") else 0
                    
                    for hashtag in hashtags:
                        hashtag_performance[hashtag].append(engagement_score)
            
            # Calculate average performance for each hashtag
            effective_hashtags = []
            for hashtag, scores in hashtag_performance.items():
                if len(scores) >= 3:  # Only consider hashtags used multiple times
                    avg_performance = statistics.mean(scores)
                    effective_hashtags.append({
                        "hashtag": hashtag,
                        "usage_count": len(scores),
                        "avg_performance": avg_performance
                    })
            
            # Sort by performance
            effective_hashtags.sort(key=lambda x: x["avg_performance"], reverse=True)
            
            return effective_hashtags[:10]  # Top 10
            
        except Exception as e:
            logger.error(f"Error analyzing hashtags: {str(e)}")
            return []
    
    async def _analyze_engagement_patterns(self, interactions: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze engagement patterns."""
        try:
            patterns = {
                "response_time_correlation": 0.0,
                "platform_effectiveness": {},
                "content_length_optimization": {},
                "interaction_type_performance": {}
            }
            
            # Analyze platform effectiveness
            platform_performance = defaultdict(list)
            for interaction in interactions:
                platform = interaction.get("platform", "unknown")
                success = 1 if interaction.get("success") else 0
                platform_performance[platform].append(success)
            
            for platform, scores in platform_performance.items():
                patterns["platform_effectiveness"][platform] = {
                    "success_rate": statistics.mean(scores) if scores else 0,
                    "total_interactions": len(scores)
                }
            
            # Analyze content length
            length_performance = defaultdict(list)
            for interaction in interactions:
                content = interaction.get("text", "") or interaction.get("content", "")
                if content:
                    length_category = self._categorize_content_length(len(content))
                    success = 1 if interaction.get("success") else 0
                    length_performance[length_category].append(success)
            
            for category, scores in length_performance.items():
                patterns["content_length_optimization"][category] = {
                    "success_rate": statistics.mean(scores) if scores else 0,
                    "sample_size": len(scores)
                }
            
            # Analyze interaction types
            type_performance = defaultdict(list)
            for interaction in interactions:
                interaction_type = interaction.get("type", "unknown")
                success = 1 if interaction.get("success") else 0
                type_performance[interaction_type].append(success)
            
            for int_type, scores in type_performance.items():
                patterns["interaction_type_performance"][int_type] = {
                    "success_rate": statistics.mean(scores) if scores else 0,
                    "frequency": len(scores)
                }
            
            return patterns
            
        except Exception as e:
            logger.error(f"Error analyzing engagement patterns: {str(e)}")
            return {}
    
    def _categorize_content_length(self, length: int) -> str:
        """Categorize content by length."""
        if length <= 50:
            return "very_short"
        elif length <= 100:
            return "short"
        elif length <= 200:
            return "medium"
        elif length <= 300:
            return "long"
        else:
            return "very_long"
    
    async def _analyze_content_themes(self, interactions: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze content themes using AI."""
        try:
            # Collect successful content
            successful_content = []
            for interaction in interactions:
                if interaction.get("success"):
                    content = interaction.get("text", "") or interaction.get("content", "")
                    if content and len(content) > 20:
                        successful_content.append(content)
            
            if not successful_content:
                return {}
            
            # Use AI to analyze themes
            analysis_prompt = f"""
            Analyze these successful social media posts and identify common themes, topics, and patterns.
            Provide a JSON response with:
            - top_themes: list of main themes/topics
            - tone_analysis: predominant tone (professional, casual, humorous, etc.)
            - common_keywords: frequently used effective keywords
            - content_patterns: structural patterns that work well
            
            Content samples (first 10):
            {json.dumps(successful_content[:10], indent=2)}
            """
            
            response = await content_generator.generate_custom_content(
                analysis_prompt,
                system_prompt="You are a content analysis expert. Respond only with valid JSON."
            )
            
            if response:
                try:
                    return json.loads(response)
                except json.JSONDecodeError:
                    pass
            
            # Fallback analysis
            return {
                "top_themes": ["business", "engagement", "social_media"],
                "tone_analysis": "professional",
                "common_keywords": [],
                "content_patterns": []
            }
            
        except Exception as e:
            logger.error(f"Error analyzing content themes: {str(e)}")
            return {}

class SalesPatternAnalyzer:
    """Analyzes sales patterns and customer behavior."""
    
    @log_async_function_calls("auto_learning")
    async def analyze_sales_patterns(self, days: int = 30) -> Dict[str, Any]:
        """
        Analyze sales patterns and customer behavior.
        
        Args:
            days: Number of days to analyze
            
        Returns:
            Sales pattern analysis
        """
        try:
            # Get leads and sales data
            all_leads = await leads_manager.get_all_leads()
            all_sales = await sales_manager.get_all_sales()
            
            # Filter by date
            cutoff_date = datetime.now() - timedelta(days=days)
            recent_leads = []
            recent_sales = []
            
            for lead in all_leads:
                created_at = lead.get("created_at")
                if created_at:
                    try:
                        lead_date = datetime.fromisoformat(created_at)
                        if lead_date > cutoff_date:
                            recent_leads.append(lead)
                    except:
                        continue
            
            for sale in all_sales:
                created_at = sale.get("created_at")
                if created_at:
                    try:
                        sale_date = datetime.fromisoformat(created_at)
                        if sale_date > cutoff_date:
                            recent_sales.append(sale)
                    except:
                        continue
            
            analysis = {
                "conversion_patterns": await self._analyze_conversion_patterns(recent_leads),
                "customer_behavior": await self._analyze_customer_behavior(recent_leads),
                "sales_timing": await self._analyze_sales_timing(recent_sales),
                "lead_quality_indicators": await self._analyze_lead_quality(recent_leads),
                "follow_up_effectiveness": await self._analyze_follow_up_patterns(recent_leads)
            }
            
            return analysis
            
        except Exception as e:
            logger.error(f"Error analyzing sales patterns: {str(e)}")
            return {}
    
    async def _analyze_conversion_patterns(self, leads: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze lead conversion patterns."""
        try:
            conversion_data = {
                "by_platform": defaultdict(lambda: {"total": 0, "converted": 0}),
                "by_qualification": defaultdict(lambda: {"total": 0, "converted": 0}),
                "by_lead_score": defaultdict(lambda: {"total": 0, "converted": 0}),
                "time_to_conversion": []
            }
            
            for lead in leads:
                platform = lead.get("platform", "unknown")
                qualification = lead.get("qualification", "unqualified")
                lead_score = lead.get("lead_score", 0)
                status = lead.get("status", "new")
                
                # Score range categorization
                score_range = self._categorize_lead_score(lead_score)
                
                # Update totals
                conversion_data["by_platform"][platform]["total"] += 1
                conversion_data["by_qualification"][qualification]["total"] += 1
                conversion_data["by_lead_score"][score_range]["total"] += 1
                
                # Update conversions
                if status == "converted":
                    conversion_data["by_platform"][platform]["converted"] += 1
                    conversion_data["by_qualification"][qualification]["converted"] += 1
                    conversion_data["by_lead_score"][score_range]["converted"] += 1
                    
                    # Calculate time to conversion
                    created_at = lead.get("created_at")
                    conversion_date = lead.get("conversion_date")
                    if created_at and conversion_date:
                        try:
                            created = datetime.fromisoformat(created_at)
                            converted = datetime.fromisoformat(conversion_date)
                            days_to_convert = (converted - created).days
                            conversion_data["time_to_conversion"].append(days_to_convert)
                        except:
                            continue
            
            # Calculate conversion rates
            patterns = {}
            
            for category, data in conversion_data.items():
                if category == "time_to_conversion":
                    if data:
                        patterns[category] = {
                            "avg_days": statistics.mean(data),
                            "median_days": statistics.median(data),
                            "min_days": min(data),
                            "max_days": max(data)
                        }
                    else:
                        patterns[category] = {}
                else:
                    patterns[category] = {}
                    for key, values in data.items():
                        total = values["total"]
                        converted = values["converted"]
                        conversion_rate = (converted / total * 100) if total > 0 else 0
                        patterns[category][key] = {
                            "total_leads": total,
                            "converted_leads": converted,
                            "conversion_rate": conversion_rate
                        }
            
            return patterns
            
        except Exception as e:
            logger.error(f"Error analyzing conversion patterns: {str(e)}")
            return {}
    
    def _categorize_lead_score(self, score: float) -> str:
        """Categorize lead score into ranges."""
        if score >= 80:
            return "80-100"
        elif score >= 60:
            return "60-79"
        elif score >= 40:
            return "40-59"
        elif score >= 20:
            return "20-39"
        else:
            return "0-19"
    
    async def _analyze_customer_behavior(self, leads: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze customer behavior patterns."""
        try:
            behavior_patterns = {
                "response_time_impact": {},
                "interaction_frequency_impact": {},
                "inquiry_quality_correlation": {},
                "platform_preferences": {}
            }
            
            # Analyze response time impact
            response_times = []
            conversions_by_response_time = []
            
            for lead in leads:
                avg_response_time = lead.get("avg_response_time_hours", 24)
                converted = 1 if lead.get("status") == "converted" else 0
                
                response_times.append(avg_response_time)
                conversions_by_response_time.append((avg_response_time, converted))
            
            # Calculate correlation (simplified)
            if conversions_by_response_time:
                fast_responders = [c for rt, c in conversions_by_response_time if rt <= 2]
                slow_responders = [c for rt, c in conversions_by_response_time if rt > 12]
                
                behavior_patterns["response_time_impact"] = {
                    "fast_response_conversion": statistics.mean(fast_responders) if fast_responders else 0,
                    "slow_response_conversion": statistics.mean(slow_responders) if slow_responders else 0,
                    "optimal_response_time": "< 2 hours" if statistics.mean(fast_responders or [0]) > statistics.mean(slow_responders or [0]) else "> 12 hours"
                }
            
            return behavior_patterns
            
        except Exception as e:
            logger.error(f"Error analyzing customer behavior: {str(e)}")
            return {}
    
    async def _analyze_sales_timing(self, sales: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze sales timing patterns."""
        try:
            timing_analysis = {
                "best_days": {},
                "best_hours": {},
                "seasonal_patterns": {}
            }
            
            daily_sales = defaultdict(int)
            hourly_sales = defaultdict(int)
            
            for sale in sales:
                created_at = sale.get("created_at")
                if created_at:
                    try:
                        sale_time = datetime.fromisoformat(created_at)
                        day_name = sale_time.strftime("%A")
                        hour = sale_time.hour
                        
                        daily_sales[day_name] += 1
                        hourly_sales[hour] += 1
                    except:
                        continue
            
            timing_analysis["best_days"] = dict(daily_sales)
            timing_analysis["best_hours"] = dict(hourly_sales)
            
            return timing_analysis
            
        except Exception as e:
            logger.error(f"Error analyzing sales timing: {str(e)}")
            return {}
    
    async def _analyze_lead_quality(self, leads: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze lead quality indicators."""
        try:
            quality_indicators = {
                "high_converting_characteristics": [],
                "low_converting_characteristics": [],
                "quality_score_effectiveness": {}
            }
            
            # Separate converted and non-converted leads
            converted_leads = [l for l in leads if l.get("status") == "converted"]
            non_converted_leads = [l for l in leads if l.get("status") != "converted"]
            
            # Analyze characteristics of converted leads
            if converted_leads:
                avg_converted_score = statistics.mean([l.get("lead_score", 0) for l in converted_leads])
                common_platforms = Counter([l.get("platform", "") for l in converted_leads])
                common_intents = Counter([l.get("intent", "") for l in converted_leads])
                
                quality_indicators["high_converting_characteristics"] = [
                    f"Average lead score: {avg_converted_score:.1f}",
                    f"Top platform: {common_platforms.most_common(1)[0][0] if common_platforms else 'N/A'}",
                    f"Common intent: {common_intents.most_common(1)[0][0] if common_intents else 'N/A'}"
                ]
            
            return quality_indicators
            
        except Exception as e:
            logger.error(f"Error analyzing lead quality: {str(e)}")
            return {}
    
    async def _analyze_follow_up_patterns(self, leads: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze follow-up effectiveness."""
        try:
            follow_up_analysis = {
                "optimal_follow_up_count": 0,
                "best_follow_up_timing": {},
                "follow_up_conversion_correlation": {}
            }
            
            # Analyze follow-up patterns for converted leads
            converted_with_follow_ups = []
            
            for lead in leads:
                if lead.get("status") == "converted":
                    interactions = lead.get("interactions", [])
                    follow_ups = [i for i in interactions if i.get("type") == "follow_up"]
                    converted_with_follow_ups.append(len(follow_ups))
            
            if converted_with_follow_ups:
                follow_up_analysis["optimal_follow_up_count"] = statistics.mean(converted_with_follow_ups)
            
            return follow_up_analysis
            
        except Exception as e:
            logger.error(f"Error analyzing follow-up patterns: {str(e)}")
            return {}

class LearningEngine:
    """Main learning engine that coordinates analysis and optimization."""
    
    def __init__(self):
        self.content_analyzer = ContentAnalyzer()
        self.sales_analyzer = SalesPatternAnalyzer()
    
    @log_async_function_calls("auto_learning")
    async def run_learning_cycle(self) -> Dict[str, Any]:
        """
        Run a complete learning cycle to analyze patterns and generate insights.
        
        Returns:
            Learning insights and recommendations
        """
        try:
            logger.info("Starting auto-learning cycle")
            
            # Analyze content performance
            content_analysis = await self.content_analyzer.analyze_content_performance()
            
            # Analyze sales patterns
            sales_analysis = await self.sales_analyzer.analyze_sales_patterns()
            
            # Generate optimization recommendations
            recommendations = await self._generate_optimization_recommendations(
                content_analysis, sales_analysis
            )
            
            # Store learning insights
            learning_insights = {
                "analysis_timestamp": datetime.now().isoformat(),
                "content_analysis": content_analysis,
                "sales_analysis": sales_analysis,
                "recommendations": recommendations,
                "next_learning_cycle": (datetime.now() + timedelta(hours=settings.learning_update_hours)).isoformat()
            }
            
            await self._store_learning_insights(learning_insights)
            
            # Update system parameters based on insights
            await self._apply_learning_insights(learning_insights)
            
            logger.info("Auto-learning cycle completed successfully")
            return learning_insights
            
        except Exception as e:
            logger.error(f"Error in learning cycle: {str(e)}")
            return {}
    
    async def _generate_optimization_recommendations(self, content_analysis: Dict[str, Any], 
                                                   sales_analysis: Dict[str, Any]) -> List[Dict[str, str]]:
        """Generate actionable optimization recommendations."""
        try:
            recommendations = []
            
            # Content optimization recommendations
            if content_analysis.get("optimal_posting_times"):
                best_hour = content_analysis["optimal_posting_times"].get("best_hour", 12)
                best_day = content_analysis["optimal_posting_times"].get("best_day", "Monday")
                recommendations.append({
                    "category": "content_timing",
                    "recommendation": f"Optimize posting schedule for {best_day}s at {best_hour}:00",
                    "impact": "high",
                    "implementation": "Update scheduler module with optimal timing"
                })
            
            # Hashtag recommendations
            effective_hashtags = content_analysis.get("effective_hashtags", [])
            if effective_hashtags:
                top_hashtags = [h["hashtag"] for h in effective_hashtags[:3]]
                recommendations.append({
                    "category": "content_hashtags",
                    "recommendation": f"Increase usage of high-performing hashtags: {', '.join(top_hashtags)}",
                    "impact": "medium",
                    "implementation": "Update content generation prompts"
                })
            
            # Platform optimization
            platform_effectiveness = content_analysis.get("engagement_patterns", {}).get("platform_effectiveness", {})
            if platform_effectiveness:
                best_platform = max(platform_effectiveness.items(), 
                                  key=lambda x: x[1].get("success_rate", 0))[0]
                recommendations.append({
                    "category": "platform_focus",
                    "recommendation": f"Increase activity on {best_platform} (highest success rate)",
                    "impact": "high",
                    "implementation": "Adjust posting frequency per platform"
                })
            
            # Sales optimization recommendations
            conversion_patterns = sales_analysis.get("conversion_patterns", {})
            if conversion_patterns.get("by_qualification"):
                best_qualification = max(conversion_patterns["by_qualification"].items(),
                                       key=lambda x: x[1].get("conversion_rate", 0))[0]
                recommendations.append({
                    "category": "lead_qualification",
                    "recommendation": f"Focus on generating more '{best_qualification}' qualified leads",
                    "impact": "high",
                    "implementation": "Adjust lead scoring criteria"
                })
            
            # Response time optimization
            customer_behavior = sales_analysis.get("customer_behavior", {})
            response_impact = customer_behavior.get("response_time_impact", {})
            if response_impact.get("optimal_response_time"):
                recommendations.append({
                    "category": "response_timing",
                    "recommendation": f"Maintain response times {response_impact['optimal_response_time']} for better conversion",
                    "impact": "medium",
                    "implementation": "Optimize automated response scheduling"
                })
            
            return recommendations
            
        except Exception as e:
            logger.error(f"Error generating optimization recommendations: {str(e)}")
            return []
    
    async def _store_learning_insights(self, insights: Dict[str, Any]):
        """Store learning insights in data store."""
        try:
            # Store current insights
            await data_store.set("learning_insights:current", insights)
            
            # Store in historical insights
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            await data_store.set(f"learning_insights:history:{timestamp}", insights, expire=86400 * 30)  # 30 days
            
            # Update insights history list
            history_list = await data_store.get("learning_insights:history_list") or []
            history_list.insert(0, {
                "timestamp": timestamp,
                "analysis_time": insights["analysis_timestamp"]
            })
            
            # Keep only last 50 insights
            history_list = history_list[:50]
            await data_store.set("learning_insights:history_list", history_list)
            
            logger.info("Learning insights stored successfully")
            
        except Exception as e:
            logger.error(f"Error storing learning insights: {str(e)}")
    
    async def _apply_learning_insights(self, insights: Dict[str, Any]):
        """Apply learning insights to optimize system parameters."""
        try:
            recommendations = insights.get("recommendations", [])
            
            # Store optimization parameters
            optimization_params = {
                "updated_at": datetime.now().isoformat(),
                "active_optimizations": []
            }
            
            for rec in recommendations:
                if rec.get("impact") == "high":
                    optimization_params["active_optimizations"].append({
                        "category": rec["category"],
                        "recommendation": rec["recommendation"],
                        "status": "pending_implementation"
                    })
            
            await data_store.set("optimization_parameters", optimization_params)
            
            # Apply specific optimizations
            await self._apply_content_optimizations(insights.get("content_analysis", {}))
            await self._apply_sales_optimizations(insights.get("sales_analysis", {}))
            
            logger.info("Learning insights applied successfully")
            
        except Exception as e:
            logger.error(f"Error applying learning insights: {str(e)}")
    
    async def _apply_content_optimizations(self, content_analysis: Dict[str, Any]):
        """Apply content-specific optimizations."""
        try:
            # Store optimal posting times
            optimal_times = content_analysis.get("optimal_posting_times", {})
            if optimal_times:
                await data_store.set("optimal_posting_schedule", optimal_times)
            
            # Store effective hashtags
            effective_hashtags = content_analysis.get("effective_hashtags", [])
            if effective_hashtags:
                hashtag_list = [h["hashtag"] for h in effective_hashtags[:10]]
                await data_store.set("recommended_hashtags", hashtag_list)
            
            # Store content themes
            content_themes = content_analysis.get("content_themes", {})
            if content_themes:
                await data_store.set("effective_content_themes", content_themes)
            
        except Exception as e:
            logger
